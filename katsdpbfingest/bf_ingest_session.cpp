/* Backend implementation of beamformer ingest, written in C++ for efficiency.
 *
 * Even though the data rates are not that high in absolute terms, careful
 * design is needed. Simply setting the HDF5 chunk size to match the heap size
 * will not be sufficient, since the heaps are small enough that this is very
 * inefficient with O_DIRECT (and O_DIRECT is needed to keep performance
 * predictable enough). The high heap rate also makes a single ring buffer with
 * entry per heap unattractive.
 *
 * Instead, the network thread assembles heaps into "slices", which span the
 * entire band. Slices are then passed through a ring buffer to the disk
 * writer thread. At present, slices also match HDF5 chunks, although if
 * desirable they could be split into smaller chunks for more efficient reads
 * of subbands (this will, however, reduce write performance).
 *
 * libhdf5 doesn't support scatter-gather, so each slice needs to be collected
 * in contiguous memory. To avoid extra copies, a custom allocator is used to
 * provision space in the slice so that spead2 will fill in the payload
 * in-place.
 */

/* Still TODO:
 * - improve libhdf5 exception handling:
 *   - put full backtrace into exception object
 *   - debug segfault in exit handlers
 * - grow the file in batches, shrink again at end?
 * - make Python code more robust to the file being corrupt?
 */

#include <memory>
#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <utility>
#include <future>
#include <cstdint>
#include <unistd.h>
#include <regex>
#include <boost/format.hpp>
#include <spead2/recv_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/recv_utils.h>
#include <spead2/common_endian.h>
#include <spead2/recv_ring_stream.h>
#include <spead2/recv_udp_ibv.h>
#include <spead2/recv_udp.h>
#include <spead2/common_memory_pool.h>
#include <spead2/common_thread_pool.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <system_error>
#include <cerrno>
#include <cstdlib>
#include <H5Cpp.h>
#include <pybind11/pybind11.h>

namespace py = pybind11;

static constexpr int ALIGNMENT = 4096;

class log_function_python
{
private:
    py::object logger;

public:
    log_function_python() = default;
    explicit log_function_python(py::object logger) : logger(std::move(logger)) {}

    void operator()(spead2::log_level level, const std::string &msg)
    {
        static const char *const level_methods[] =
        {
            "warning",
            "info",
            "debug"
        };
        unsigned int level_idx = static_cast<unsigned int>(level);
        assert(level_idx < sizeof(level_methods) / sizeof(level_methods[0]));
        py::gil_scoped_acquire gil;
        logger.attr(level_methods[level_idx])("%s", msg);
    }
};

static log_function_python logger;

/**
 * Recursively push a variadic list of arguments into a @c boost::format. This
 * is copied from the spead2 codebase.
 */
static void apply_format(boost::format &formatter)
{
}

template<typename T0, typename... Ts>
static void apply_format(boost::format &formatter, T0 &&arg0, Ts&&... args)
{
    formatter % std::forward<T0>(arg0);
    apply_format(formatter, std::forward<Ts>(args)...);
}

template<typename... Ts>
static void log_format(spead2::log_level level, const std::string &format, Ts&&... args)
{
    boost::format formatter(format);
    apply_format(formatter, args...);
    logger(level, formatter.str());
}

template<typename T>
struct free_delete
{
    void operator()(T *ptr) const
    {
        free(ptr);
    }
};

template<typename T>
using aligned_ptr = std::unique_ptr<T[], free_delete<T>>;

/**
 * Allocate memory that is aligned to a multiple of @c ALIGNMENT. This is used
 * with O_DIRECT.
 */
template<typename T>
static aligned_ptr<T> make_aligned(std::size_t elements)
{
    void *ptr = aligned_alloc(ALIGNMENT, elements * sizeof(T));
    if (!ptr)
        throw std::bad_alloc();
    return std::unique_ptr<T[], free_delete<T>>(static_cast<T*>(ptr));
}

/**
 * Return a vector that can be passed to a @c spead2::thread_pool constructor
 * to bind to core @a affinity is non-negative.
 */
std::vector<int> affinity_vector(int affinity)
{
    if (affinity < 0)
        return {};
    else
        return {affinity};
}

/**
 * Generic class (using Curiously Recursive Template Pattern) for managing an
 * (unbounded) sequence of items, each of which is built up from smaller
 * items, which do not necessarily arrive strictly in order. This class
 * manages a fixed-size window of slots. The derived class can ask for access
 * to a specific element of the sequence, and if necessary the window is
 * advanced, flushing trailing items. The window is never retracted; if the
 * caller wants to access an item that is too old, it is refused.
 *
 * The subclass must provide a @c flush method to process an item from the
 * trailing edge and then reset its state. Note that @c flush may be called on
 * items that have never been accessed with @ref get, so subclasses must
 * handle this.
 */
template<typename T, typename Derived>
class window
{
public:
    typedef T value_type;

private:
    /**
     * The window itself. Item @c i is stored in index
     * <code>i % window_size</code>, provided that
     * <code>start &lt;= i &lt; start + window_size</code>.
     */
    std::vector<T> slots;
    std::size_t oldest = 0;      ///< Index into @ref slots of oldest item
    std::int64_t start = 0;      ///< ID of oldest item in window

    void flush_oldest();

public:
    // The args are passed to the constructor for T to construct the slots
    template<typename... Args>
    explicit window(std::size_t window_size, Args&&... args);

    /// Returns pointer to the slot for @a id, or @c nullptr if the window has moved on
    T *get(std::int64_t id);

    /// Flush the next @a window_size items (even if they have never been requested)
    void flush_all();
};

template<typename T, typename Derived>
template<typename... Args>
window<T, Derived>::window(std::size_t window_size, Args&&... args)
{
    slots.reserve(window_size);
    for (std::size_t i = 0; i < window_size; i++)
        slots.emplace_back(std::forward<Args>(args)...);
}

template<typename T, typename Derived>
void window<T, Derived>::flush_oldest()
{
    T &item = slots[oldest];
    static_cast<Derived *>(this)->flush(item);
    oldest++;
    if (oldest == slots.size())
        oldest = 0;
    start++;
}

template<typename T, typename Derived>
T *window<T, Derived>::get(std::int64_t id)
{
    if (id < start)
        return nullptr;
    // TODO: fast-forward mechanism for big differences?
    while (id - start >= std::int64_t(slots.size()))
        flush_oldest();
    std::size_t pos = id % slots.size();
    return &slots[pos];
}

template<typename T, typename Derived>
void window<T, Derived>::flush_all()
{
    for (std::size_t i = 0; i < slots.size(); i++)
        flush_oldest();
}


/**
 * Creates a dataset transfer property list that can be used for writing chunks
 * of size @a size. It is based on examining the code for H5DOwrite_chunk, but
 * doing it directly actually makes it easier to do things using the C++ API,
 * as well as avoiding the need to continually flip the flag on and off.
 */
static H5::DSetMemXferPropList make_dxpl_direct(std::size_t size)
{
    hbool_t direct_write = true;
    H5::DSetMemXferPropList dxpl;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_FLAG_NAME, &direct_write);
    // The size of this property changed somewhere between 1.8.11 and 1.8.17
    std::size_t property_size = dxpl.getPropSize(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME);
    if (property_size == sizeof(size))
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME, &size);
    else if (property_size == sizeof(std::uint32_t))
    {
        std::uint32_t size32 = size;
        assert(size32 == size);
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME, &size32);
    }
    return dxpl;
}

// Parse the shape from either the shape field or the numpy header
static std::vector<spead2::s_item_pointer_t> get_shape(const spead2::descriptor &descriptor)
{
    using spead2::s_item_pointer_t;

    if (!descriptor.numpy_header.empty())
    {
        // Slightly hacky approach to find out the shape (without
        // trying to implement a Python interpreter)
        std::regex expr("['\"]shape['\"]:\\s*\\(([^)]*)\\)");
        std::smatch what;
        if (regex_search(descriptor.numpy_header, what, expr))
        {
            std::vector<s_item_pointer_t> shape;
            std::string inside = what[1];
            std::replace(inside.begin(), inside.end(), ',', ' ');
            std::istringstream tokeniser(inside);
            s_item_pointer_t cur;
            while (tokeniser >> cur)
            {
                shape.push_back(cur);
            }
            if (!tokeniser.eof())
                throw std::runtime_error("could not parse shape (" + inside + ")");
            return shape;
        }
        else
            throw std::runtime_error("could not parse numpy header " + descriptor.numpy_header);
    }
    else
        return descriptor.shape;
}

/**
 * Storage for all the heaps that share a timestamp.
 */
struct slice
{
    std::int64_t timestamp = -1;       ///< Timestamp from the heap
    std::int64_t spectrum = -1;        ///< Number of spectra since start
    unsigned int n_present = 0;        ///< Number of 1 bits in @a present
    aligned_ptr<std::uint8_t> data;    ///< Payload: channel-major, time-minor
    std::vector<bool> present;         ///< Bitmask of present heaps
};

class hdf5_bf_raw_writer
{
private:
    const int channels;
    const int spectra_per_heap;
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;

public:
    hdf5_bf_raw_writer(H5::CommonFG &parent, int channels,
                       int spectra_per_heap,
                       const char *name);

    void add(const slice &c);
};

hdf5_bf_raw_writer::hdf5_bf_raw_writer(
    H5::CommonFG &parent, int channels, int spectra_per_heap, const char *name)
    : channels(channels), spectra_per_heap(spectra_per_heap),
    dxpl(make_dxpl_direct(std::size_t(channels) * spectra_per_heap * 2))
{
    hsize_t dims[3] = {hsize_t(channels), 0, 2};
    hsize_t maxdims[3] = {hsize_t(channels), H5S_UNLIMITED, 2};
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_heap), 2};
    H5::DataSpace file_space(3, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(3, chunk);
    std::int8_t fill = 0;
    dcpl.setFillValue(H5::PredType::NATIVE_INT8, &fill);
    dataset = parent.createDataSet(name, H5::PredType::STD_I8BE, file_space, dcpl);
}

void hdf5_bf_raw_writer::add(const slice &s)
{
    hsize_t end = s.spectrum + spectra_per_heap;
    hsize_t new_size[3] = {hsize_t(channels), end, 2};
    dataset.extend(new_size);
    const hsize_t offset[3] = {0, hsize_t(s.spectrum), 0};
    const hsize_t *offset_ptr = offset;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
    dataset.write(s.data.get(), H5::PredType::STD_I8BE, H5::DataSpace::ALL, H5::DataSpace::ALL, dxpl);
}

class hdf5_timestamps_writer
{
private:
    static constexpr hsize_t chunk = 1048576;
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;
    std::unique_ptr<std::uint64_t[], free_delete<std::uint64_t>> buffer;
    hsize_t n_buffer = 0;
    hsize_t n_written = 0;

    void flush();
public:
    const int spectra_per_heap;
    const std::uint64_t ticks_between_spectra;

    hdf5_timestamps_writer(H5::CommonFG &parent, int spectra_per_heap,
                           std::uint64_t ticks_between_spectra, const char *name);
    ~hdf5_timestamps_writer();
    // Add a heap's worth of timestamps
    void add(std::uint64_t timestamp);
};

constexpr hsize_t hdf5_timestamps_writer::chunk;

static void set_string_attribute(H5::H5Object &location, const std::string &name, const std::string &value)
{
    H5::DataSpace scalar;
    H5::StrType type(H5::PredType::C_S1, value.size());
    H5::Attribute attribute = location.createAttribute(name, type, scalar);
    attribute.write(type, value);
}

hdf5_timestamps_writer::hdf5_timestamps_writer(
    H5::CommonFG &parent, int spectra_per_heap,
    std::uint64_t ticks_between_spectra, const char *name)
    : dxpl(make_dxpl_direct(chunk * sizeof(std::uint64_t))),
    spectra_per_heap(spectra_per_heap),
    ticks_between_spectra(ticks_between_spectra)
{
    hsize_t dims[1] = {0};
    hsize_t maxdims[1] = {H5S_UNLIMITED};
    H5::DataSpace file_space(1, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(1, &chunk);
    std::uint64_t fill = 0;
    dcpl.setFillValue(H5::PredType::NATIVE_UINT64, &fill);
    dataset = parent.createDataSet(
        name, H5::PredType::NATIVE_UINT64, file_space, dcpl);
    buffer = make_aligned<std::uint64_t>(chunk);
    n_buffer = 0;
    set_string_attribute(dataset, "timestamp_reference", "start");
    set_string_attribute(dataset, "timestamp_type", "adc");
}

hdf5_timestamps_writer::~hdf5_timestamps_writer()
{
    if (!std::uncaught_exception() && n_buffer > 0)
        flush();
}

void hdf5_timestamps_writer::flush()
{
    hsize_t new_size = n_written + n_buffer;
    dataset.extend(&new_size);
    const hsize_t offset[1] = {n_written};
    const hsize_t *offset_ptr = offset;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
    if (n_buffer < chunk)
    {
        // Pad extra space with zeros - shouldn't matter, but this case
        // only arises when closing the file so should be cheap
        std::memset(buffer.get() + n_buffer, 0, (chunk - n_buffer) * sizeof(std::uint64_t));
    }
    dataset.write(buffer.get(), H5::PredType::NATIVE_UINT64, H5S_ALL, H5S_ALL, dxpl);
    n_written += n_buffer;
    n_buffer = 0;
}

void hdf5_timestamps_writer::add(std::uint64_t timestamp)
{
    for (int i = 0; i < spectra_per_heap; i++)
    {
        buffer[n_buffer++] = timestamp;
        timestamp += ticks_between_spectra;
    }
    assert(n_buffer <= chunk);
    if (n_buffer == chunk)
        flush();
}

static constexpr std::uint8_t data_lost = 1 << 3;

/**
 * Memory storage for an HDF5 chunk of flags data. This covers the whole band
 * and also many heaps in time.
 */
struct flags_chunk
{
    std::int64_t spectrum = -1;
    aligned_ptr<std::uint8_t> data;

    explicit flags_chunk(std::size_t size)
        : data(make_aligned<std::uint8_t>(size))
    {
        std::memset(data.get(), data_lost, size);
    }
};

class hdf5_flags_writer : private window<flags_chunk, hdf5_flags_writer>
{
private:
    friend class window<flags_chunk, hdf5_flags_writer>;

    std::size_t spectra_per_heap;
    std::size_t heaps_per_slice;
    std::size_t heaps_per_chunk;
    std::size_t slices_per_chunk;
    hsize_t n_slices = 0;    ///< Total slices seen (including skipped ones)
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;

    static std::size_t compute_chunk_size(int heaps_per_slice);
    void flush(flags_chunk &chunk);
public:
    hdf5_flags_writer(H5::CommonFG &parent, int heaps_per_slice, int spectra_per_heap,
                      const char *name);
    ~hdf5_flags_writer();
    void add(const slice &s);
};

std::size_t hdf5_flags_writer::compute_chunk_size(int heaps_per_slice)
{
    // Make each slice about 4MiB, rounding up if needed
    std::size_t slices = (4 * 1024 * 1024 + heaps_per_slice - 1) / heaps_per_slice;
    return slices * heaps_per_slice;
}

hdf5_flags_writer::hdf5_flags_writer(
    H5::CommonFG &parent, int heaps_per_slice, int spectra_per_heap,
    const char *name)
    : window<flags_chunk, hdf5_flags_writer>(1, compute_chunk_size(heaps_per_slice)),
    spectra_per_heap(spectra_per_heap),
    heaps_per_slice(heaps_per_slice),
    heaps_per_chunk(compute_chunk_size(heaps_per_slice)),
    slices_per_chunk(heaps_per_chunk / heaps_per_slice),
    dxpl(make_dxpl_direct(heaps_per_chunk))
{
    hsize_t dims[2] = {hsize_t(heaps_per_slice), 0};
    hsize_t maxdims[2] = {dims[0], H5S_UNLIMITED};
    hsize_t chunk[2] = {dims[0], hsize_t(slices_per_chunk)};
    H5::DataSpace file_space(2, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(2, chunk);
    dcpl.setFillValue(H5::PredType::NATIVE_UINT8, &data_lost);
    dataset = parent.createDataSet(name, H5::PredType::STD_U8BE, file_space, dcpl);
}

hdf5_flags_writer::~hdf5_flags_writer()
{
    if (!std::uncaught_exception())
        flush_all();
}

void hdf5_flags_writer::flush(flags_chunk &chunk)
{
    if (chunk.spectrum != -1)
    {
        hsize_t new_size[2] = {hsize_t(heaps_per_slice), n_slices};
        dataset.extend(new_size);
        const hsize_t offset[2] = {0, hsize_t(chunk.spectrum / spectra_per_heap)};
        const hsize_t *offset_ptr = offset;
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
        dataset.write(chunk.data.get(), H5::PredType::NATIVE_UINT8, H5S_ALL, H5S_ALL, dxpl);
    }
    chunk.spectrum = -1;
    std::memset(chunk.data.get(), data_lost, heaps_per_chunk);
}

void hdf5_flags_writer::add(const slice &s)
{
    std::int64_t slice_id = s.spectrum / spectra_per_heap;
    std::int64_t id = slice_id / slices_per_chunk;
    flags_chunk *chunk = get(id);
    assert(chunk != nullptr);  // we are given slices in-order, so cannot be behind the window
    std::size_t offset = slice_id - id * slices_per_chunk;
    for (std::size_t i = 0; i < s.present.size(); i++)
        chunk->data[i * slices_per_chunk + offset] = s.present[i] ? 0 : data_lost;
    chunk->spectrum = id * spectra_per_heap * slices_per_chunk;
    n_slices = slice_id + 1;
}

class hdf5_writer
{
private:
    std::int64_t past_end_timestamp = -1;
    H5::H5File file;
    H5::Group group;
    hdf5_bf_raw_writer bf_raw;
    hdf5_timestamps_writer captured_timestamps, all_timestamps;
    hdf5_flags_writer flags;

    static H5::FileAccPropList make_fapl(bool direct);

public:
    hdf5_writer(const std::string &filename, bool direct,
                int channels, int channels_per_heap, int spectra_per_heap,
                std::int64_t ticks_between_spectra)
        : file(filename, H5F_ACC_TRUNC, H5::FileCreatPropList::DEFAULT, make_fapl(direct)),
        group(file.createGroup("Data")),
        bf_raw(group, channels, spectra_per_heap, "bf_raw"),
        captured_timestamps(group, spectra_per_heap, ticks_between_spectra, "captured_timestamps"),
        all_timestamps(group, spectra_per_heap, ticks_between_spectra, "timestamps"),
        flags(group, channels / channels_per_heap, spectra_per_heap, "flags")
    {
        H5::DataSpace scalar;
        // 1.8.11 doesn't have the right C++ wrapper for this to work, so we
        // duplicate its work
        hid_t attr_id = H5Acreate2(
            file.getId(), "version", H5::PredType::NATIVE_INT32.getId(),
            scalar.getId(), H5P_DEFAULT, H5P_DEFAULT);
        if (attr_id < 0)
            throw H5::AttributeIException("createAttribute", "H5Acreate2 failed");
        H5::Attribute version_attr(attr_id);
        /* Release the ref created by H5Acreate2 (version_attr has its own).
         * HDF5 1.8.11 has a bug where version_attr doesn't get its own
         * reference, so to handle both cases we have to check the current
         * value.
         */
        if (version_attr.getCounter() > 1)
            version_attr.decRefCount();
        const std::int32_t version = 3;
        version_attr.write(H5::PredType::NATIVE_INT32, &version);
    }

    void add(const slice &s);

    int get_fd() const;
};

H5::FileAccPropList hdf5_writer::make_fapl(bool direct)
{
    H5::FileAccPropList fapl;
    if (direct)
    {
#ifdef H5_HAVE_DIRECT
        if (H5Pset_fapl_direct(fapl.getId(), ALIGNMENT, ALIGNMENT, 128 * 1024) < 0)
            throw H5::PropListIException("hdf5_writer::make_fapl", "H5Pset_fapl_direct failed");
#else
        throw std::runtime_error("H5_HAVE_DIRECT not defined");
#endif
    }
    else
    {
        fapl.setSec2();
    }
    // Older versions of libhdf5 are missing the C++ version setLibverBounds
#ifdef H5F_LIBVER_18
    const auto version = H5F_LIBVER_18;
#else
    const auto version = H5F_LIBVER_LATEST;
#endif
    if (H5Pset_libver_bounds(fapl.getId(), version, version) < 0)
        throw H5::PropListIException("FileAccPropList::setLibverBounds", "H5Pset_libver_bounds failed");
    fapl.setAlignment(ALIGNMENT, ALIGNMENT);
    fapl.setFcloseDegree(H5F_CLOSE_SEMI);
    return fapl;
}

void hdf5_writer::add(const slice &s)
{
    if (past_end_timestamp == -1)
        past_end_timestamp = s.timestamp;
    while (past_end_timestamp <= s.timestamp)
    {
        all_timestamps.add(past_end_timestamp);
        past_end_timestamp += all_timestamps.ticks_between_spectra * all_timestamps.spectra_per_heap;
    }
    if (s.n_present == s.present.size())
        captured_timestamps.add(s.timestamp);
    bf_raw.add(s);
    flags.add(s);
}

int hdf5_writer::get_fd() const
{
    void *fd_ptr;
    file.getVFDHandle(&fd_ptr);
    return *reinterpret_cast<int *>(fd_ptr);
}


struct session_config
{
    std::string filename;
    std::vector<boost::asio::ip::udp::endpoint> endpoints;
    boost::asio::ip::address interface_address;

    std::size_t buffer_size = 32 * 1024 * 1024;
    int live_heaps_per_substream = 2;
    int ring_slots = 128;
    bool ibv = false;
    int comp_vector = 0;
    int network_affinity = -1;

    int disk_affinity = -1;
    bool direct = false;

    /* Metadata derived from telescope state. If left at defaults, it will
     * be extracted from metadata SPEAD items.
     */
    std::int64_t ticks_between_spectra = -1;
    int channels = -1;
    int spectra_per_heap = -1;
    int channels_per_heap = -1;
    /// Whether to use metadata in the SPEAD stream
    bool spead_metadata = true;

    explicit session_config(const std::string &filename);
    void add_endpoint(const std::string &bind_host, std::uint16_t port);
    std::string get_interface_address() const;
    void set_interface_address(const std::string &address);
};

session_config::session_config(const std::string &filename)
    : filename(filename)
{
}

void session_config::add_endpoint(const std::string &bind_host, std::uint16_t port)
{
    endpoints.emplace_back(boost::asio::ip::address_v4::from_string(bind_host), port);
}

std::string session_config::get_interface_address() const
{
    return interface_address.to_string();
}

void session_config::set_interface_address(const std::string &address)
{
    interface_address = boost::asio::ip::address_v4::from_string(address);
}


class receiver;

/**
 * Wrapper around base stream class that forwards to a receiver.
 */
class bf_stream : public spead2::recv::stream
{
private:
    receiver &recv;

    virtual void heap_ready(spead2::recv::live_heap &&heap) override;
    virtual void stop_received() override;

public:
    bf_stream(receiver &recv, std::size_t max_heaps);
    virtual ~bf_stream() override { stop(); }
};

/**
 * Allocator that returns pointers inside slices.
 *
 * It retains a pointer to the stream, but does not use this pointer while
 * freeing. It is thus safe to free the stream even if there are still
 * allocated values outstanding.
 *
 * If it allocates memory from a slice, it sets a non-zero value in the
 * user field.
 */
class bf_raw_allocator : public spead2::memory_allocator
{
private:
    receiver &recv;

    virtual void free(std::uint8_t *ptr, void *user) override;

public:
    explicit bf_raw_allocator(receiver &recv);

    virtual pointer allocate(std::size_t size, void *hint) override;
};

/**
 * Collects data from the network, using custom stream classes. It has a
 * built-in thread pool with one thread, and runs almost entirely on that
 * thread.
 *
 * Class bf_stream is a thin stream wrapper that calls back into this class to
 * handle the received heaps.
 */
class receiver : private window<slice, receiver>
{
private:
    friend class bf_stream;
    friend class bf_raw_allocator;
    friend class window<slice, receiver>;

    enum class state_t
    {
        METADATA,     ///< Waiting for metadata
        DATA,         ///< Receiving data
        STOP          ///< Have seen stop packet or been asked to stop
    };

    const session_config config;
    bool use_ibv = false;

    /// Depth of window
    static constexpr std::size_t window_size = 2;

    // Metadata extracted from the stream or the session_config
    std::int64_t ticks_between_spectra = -1;
    int channels = -1;
    int spectra_per_heap = -1;
    int channels_per_heap = -1;
    int bf_raw_id = 0x5000;
    int timestamp_id = 0x1600;
    int frequency_id = 0x4103;
    int ticks_between_spectra_id = 0x104A;
    int channels_id = 0x1009;

    state_t state = state_t::METADATA;
    std::int64_t first_timestamp = -1;

    // Values computed from metadata by prepare_for_data
    std::size_t payload_size = 0;

    spead2::thread_pool worker;
    bf_stream stream;

    /// Create a single fully-allocated slice
    slice make_slice();

    /// Add the readers to the already-allocated stream
    void emplace_readers();

    /**
     * Process a timestamp and channel number from a heap into more useful
     * indices. Note: this function modifies state by settings @ref
     * first_timestamp if this is the first (valid) call.
     *
     * @param timestamp        ADC timestamp
     * @param channel          Channel number of first channel in heap
     * @param[out] spectrum    Index of first spectrum in heap, counting from 0
     *                         for first heap
     * @param[out] heap_offset Byte offset from start of slice data for this heap
     * @param[out] present_idx Position in @ref slice::present to record this heap
     *
     * @retval true  if @a timestamp and @a channel are valid
     * @retval false otherwise, and a message is logged
     */
    bool parse_timestamp_channel(
        std::int64_t timestamp, int channel,
        std::int64_t &spectrum, std::size_t &heap_offset, std::size_t &present_idx);

    /**
     * Obtain a pointer to an allocated slice. It returns @c nullptr if the
     * timestamp is too far in the past.
     *
     * This can block if @c free_ring is empty.
     */
    slice *get_slice(std::int64_t timestamp, std::int64_t spectrum);

    /**
     * Find space within a slice. This is the backing implementation for
     * @ref bf_raw_allocator.
     *
     * If necessary, this pushes to the ring and pulls from the free ring, so
     * it can block.
     *
     * @return  A pointer to existing memory, or @c nullptr if this is not a
     *          valid data heap.
     */
    std::uint8_t *allocate(std::size_t size, const spead2::recv::packet_header &packet);

    /**
     * Checks whether all necessary metadata has been received. Once this is
     * true, we are ready to move to state @ref DATA via
     * @ref prepare_for_data.
     */
    bool metadata_ready() const;

    /// Called once the metadata is received, to switch to data-receiving mode
    void prepare_for_data();

    /// Flush a single slice to the ringbuffer, if it has data
    void flush(slice &s);

    void process_metadata(const spead2::recv::heap &h);
    void process_data(const spead2::recv::heap &h);

    /// Called by bf_stream::heap_ready
    void heap_ready(const spead2::recv::heap &heap);
    /// Called by bf_stream::stop_received
    void stop_received();

public:
    /**
     * Filled (or partially filled) slices. These are guaranteed to be provided
     * to the consumer in order. The first element pushed is a
     * default-constructed slice, indicating only that the metadata is ready.
     */
    spead2::ringbuffer<slice> ring;

    /**
     * The consumer puts processed rings back here. It is used as a source of
     * pre-allocated objects.
     */
    spead2::ringbuffer<slice> free_ring;

    /* Accessors for metadata. These can only be accessed once
     * the metadata is ready.
     *
     * Note: the asserts are technically race conditions, because the
     * state can still mutate to STOP in another thread.
     */
#define METADATA_ACCESSOR(name) \
    decltype(name) get_ ## name() const \
    {                                               \
        assert(state != state_t::METADATA);         \
        return name;                                \
    }
    METADATA_ACCESSOR(channels)
    METADATA_ACCESSOR(channels_per_heap)
    METADATA_ACCESSOR(spectra_per_heap)
    METADATA_ACCESSOR(ticks_between_spectra)
    METADATA_ACCESSOR(payload_size)
#undef METADATA_ACCESSOR

    /**
     * Retrieve first timestamp, or -1 if no data was received.
     * It is only valid to call this once the receiver has been stopped.
     */
    std::int64_t get_first_timestamp() const
    {
        assert(state == state_t::STOP);
        return first_timestamp;
    }

    explicit receiver(const session_config &config);
    ~receiver();

    /// Stop immediately, without flushing any slices
    void stop();

    /// Asynchronously stop, allowing buffered slices to flush
    void graceful_stop();
};

constexpr std::size_t receiver::window_size;

bf_stream::bf_stream(receiver &recv, std::size_t max_heaps)
    : spead2::recv::stream(recv.worker, 0, max_heaps),
    recv(recv)
{
}

void bf_stream::heap_ready(spead2::recv::live_heap &&heap)
{
    if (!heap.is_contiguous())
        return;
    recv.heap_ready(spead2::recv::heap(std::move(heap)));
}

void bf_stream::stop_received()
{
    spead2::recv::stream::stop_received();
    recv.stop_received();
}


bf_raw_allocator::bf_raw_allocator(receiver &recv) : recv(recv)
{
}

bf_raw_allocator::pointer bf_raw_allocator::allocate(std::size_t size, void *hint)
{
    std::uint8_t *ptr = nullptr;
    if (hint)
        ptr = recv.allocate(size, *reinterpret_cast<const spead2::recv::packet_header *>(hint));
    if (ptr)
        return pointer(ptr, deleter(shared_from_this(), (void *) std::uintptr_t(true)));
    else
        return spead2::memory_allocator::allocate(size, hint);
}

void bf_raw_allocator::free(std::uint8_t *ptr, void *user)
{
    if (!user)
        delete[] ptr;
}


slice receiver::make_slice()
{
    slice s;
    std::size_t slice_size = 2 * spectra_per_heap * channels;
    std::size_t present_size = channels / channels_per_heap;
    s.data = make_aligned<std::uint8_t>(slice_size);
    // Fill the data just to pre-fault it
    std::memset(s.data.get(), 0, slice_size);
    s.present.resize(present_size);
    return s;
}

void receiver::emplace_readers()
{
    std::ostringstream endpoints_str;
    bool first = true;
    for (const auto &endpoint : config.endpoints)
    {
        if (!first)
            endpoints_str << ',';
        first = false;
        endpoints_str << endpoint;
    }
#if SPEAD2_USE_IBV
    if (use_ibv)
    {
        log_format(spead2::log_level::info, "Listening on %1% with interface %2% using ibverbs",
                   endpoints_str.str(), config.interface_address);
        stream.emplace_reader<spead2::recv::udp_ibv_reader>(
            config.endpoints, config.interface_address,
            spead2::recv::udp_ibv_reader::default_max_size,
            config.buffer_size,
            config.comp_vector);
    }
    else
#endif
    {
        if (!config.interface_address.is_unspecified())
        {
            log_format(spead2::log_level::info, "Listening on %1% with interface %2%",
                       endpoints_str.str(), config.interface_address);
            for (const auto &endpoint : config.endpoints)
                stream.emplace_reader<spead2::recv::udp_reader>(
                    endpoint, spead2::recv::udp_reader::default_max_size, config.buffer_size,
                    config.interface_address);
        }
        else
        {
            log_format(spead2::log_level::info, "Listening on %1%", endpoints_str.str());
            for (const auto &endpoint : config.endpoints)
                stream.emplace_reader<spead2::recv::udp_reader>(
                    endpoint, spead2::recv::udp_reader::default_max_size, config.buffer_size);
        }
    }
}

bool receiver::metadata_ready() const
{
    // frequency_id is excluded, since v3 of the ICD does not include it.
    logger(spead2::log_level::info, "checking whether metadata is ready");
    log_format(spead2::log_level::info,
               "channels=%1% channels_per_heap=%2% spectra_per_heap=%3% "
               "ticks_between_spectra=%4% bf_raw_id=%5% timestamp_id=%6%",
               channels, channels_per_heap, spectra_per_heap,
               ticks_between_spectra, bf_raw_id, timestamp_id);
    bool ready = channels > 0 && channels_per_heap > 0
        && spectra_per_heap > 0 && ticks_between_spectra > 0
        && bf_raw_id != -1 && timestamp_id != -1;
    logger(spead2::log_level::info, ready ? "metadata is ready" : "metadata is not ready");
    return ready;
}

void receiver::prepare_for_data()
{
    payload_size = 2 * spectra_per_heap * channels_per_heap;

    for (std::size_t i = 0; i < window_size + config.ring_slots + 1; i++)
        free_ring.push(make_slice());

    stream.set_memcpy(spead2::MEMCPY_NONTEMPORAL);
    std::shared_ptr<spead2::memory_allocator> allocator =
        std::make_shared<bf_raw_allocator>(*this);
    stream.set_memory_allocator(std::move(allocator));

    state = state_t::DATA;
    ring.emplace();  // sentinel to indicate that the metadata is ready
}

void receiver::process_metadata(const spead2::recv::heap &h)
{
    for (const auto &descriptor : h.get_descriptors())
    {
        if (descriptor.name == "bf_raw")
        {
            auto shape = get_shape(descriptor);
            if (shape.size() == 3 && shape[2] == 2)
            {
                channels_per_heap = shape[0];
                spectra_per_heap = shape[1];
            }
            bf_raw_id = descriptor.id;
        }
        else if (descriptor.name == "timestamp")
            timestamp_id = descriptor.id;
        else if (descriptor.name == "ticks_between_spectra")
            ticks_between_spectra_id = descriptor.id;
        else if (descriptor.name == "n_chans")
            channels_id = descriptor.id;
        else if (descriptor.name == "frequency")
            frequency_id = descriptor.id;
    }
    if (config.spead_metadata)
    {
        for (const auto item : h.get_items())
        {
            if (item.id == ticks_between_spectra_id)
                ticks_between_spectra = item.immediate_value;
            else if (item.id == channels_id)
                channels = item.immediate_value;
        }
    }

    if (metadata_ready())
        prepare_for_data();
}

bool receiver::parse_timestamp_channel(
    std::int64_t timestamp, int channel,
    std::int64_t &spectrum,
    std::size_t &heap_offset, std::size_t &present_idx)
{
    if (timestamp < first_timestamp)
    {
        log_format(spead2::log_level::warning, "timestamp %1% pre-dates start %2%, discarding",
                   timestamp, first_timestamp);
        return false;
    }
    std::int64_t rel = (first_timestamp == -1) ? 0 : timestamp - first_timestamp;
    if (rel % (ticks_between_spectra * spectra_per_heap) != 0)
    {
        log_format(spead2::log_level::warning, "timestamp %1% is not properly aligned to %2%, discarding",
                   timestamp, ticks_between_spectra * spectra_per_heap);
        return false;
    }
    if (channel % channels_per_heap != 0)
    {
        log_format(spead2::log_level::warning, "frequency %1% is not properly aligned to %2%, discarding",
                   channel, channels_per_heap);
        return false;
    }
    if (channel < 0 || channel >= channels)
    {
        log_format(spead2::log_level::warning, "frequency %1% is outside of range [0, %2%), discarding",
                   channel, channels);
        return false;
    }

    spectrum = rel / ticks_between_spectra;
    heap_offset = 2 * channel * spectra_per_heap;
    present_idx = channel / channels_per_heap;

    if (first_timestamp == -1)
        first_timestamp = timestamp;
    return true;
}

slice *receiver::get_slice(std::int64_t timestamp, std::int64_t spectrum)
{
    try
    {
        slice *s = get(spectrum / spectra_per_heap);
        if (!s)
        {
            logger(spead2::log_level::warning, "Timestamp went backwards");
            return nullptr;
        }
        if (!s->data)
        {
            *s = free_ring.pop();
            s->timestamp = timestamp;
            s->spectrum = spectrum;
        }
        return s;
    }
    catch (spead2::ringbuffer_stopped)
    {
        return nullptr;
    }
}

std::uint8_t *receiver::allocate(std::size_t size, const spead2::recv::packet_header &packet)
{
    if (state != state_t::DATA || size != payload_size)
        return nullptr;
    spead2::recv::pointer_decoder decoder(packet.heap_address_bits);
    // Try to extract the timestamp and frequency
    std::int64_t timestamp = -1;
    int channel = 0;
    for (int i = 0; i < packet.n_items; i++)
    {
        spead2::item_pointer_t pointer = spead2::load_be<spead2::item_pointer_t>(packet.pointers + i * sizeof(pointer));
        if (decoder.is_immediate(pointer))
        {
            int id = decoder.get_id(pointer);
            if (id == timestamp_id)
                timestamp = decoder.get_immediate(pointer);
            else if (id == frequency_id)
                channel = decoder.get_immediate(pointer);
        }
    }
    if (timestamp != -1)
    {
        // It's a data heap, so we should be able to use it
        std::int64_t spectrum;
        std::size_t heap_offset, present_idx;
        if (parse_timestamp_channel(timestamp, channel,
                                    spectrum, heap_offset, present_idx))
        {
            slice *s = get_slice(timestamp, spectrum);
            if (s)
                return s->data.get() + heap_offset;
        }
    }
    return nullptr;
}

void receiver::flush(slice &s)
{
    if (s.data)
    {
        // If any heaps got lost, fill them with zeros
        if (s.n_present != s.present.size())
        {
            std::size_t offset = 0;
            for (std::size_t i = 0; i < s.present.size(); i++, offset += payload_size)
                if (!s.present[i])
                    std::memset(s.data.get() + offset, 0, payload_size);
        }
        ring.push(std::move(s));
    }
    s.spectrum = -1;
    s.n_present = 0;
    // Clear all the bits by resizing down to zero then back to original size
    auto orig_size = s.present.size();
    s.present.clear();
    s.present.resize(orig_size);
}

void receiver::process_data(const spead2::recv::heap &h)
{
    std::int64_t timestamp = -1;
    int channel = 0;
    const spead2::recv::item *data_item = nullptr;
    for (const auto &item : h.get_items())
    {
        if (item.id == timestamp_id)
            timestamp = item.immediate_value;
        else if (item.id == frequency_id)
            channel = item.immediate_value;
        else if (item.id == bf_raw_id)
            data_item = &item;
    }
    // Metadata heaps won't have a timestamp, and metadata will continue to
    // arrive after we've seen the initial metadata heap.
    if (timestamp == -1 || data_item == nullptr)
        return;

    std::int64_t spectrum;
    std::size_t heap_offset, present_idx;
    if (!parse_timestamp_channel(timestamp, channel, spectrum, heap_offset, present_idx))
        return;
    if (data_item->length != payload_size)
    {
        log_format(spead2::log_level::warning, "bf_raw item has wrong length (%1% != %2%), discarding",
                   data_item->length, payload_size);
        return;
    }

    slice *s = get_slice(timestamp, spectrum);
    if (!s)
        return;      // Chunk has been flushed already, or we have been stopped

    std::uint8_t *ptr = s->data.get() + heap_offset;
    if (data_item->ptr != ptr)
    {
        logger(spead2::log_level::warning, "heap was not reconstructed in-place");
        std::memcpy(ptr, data_item->ptr, payload_size);
    }
    if (!s->present[present_idx])
    {
        s->n_present++;
        s->present[present_idx] = true;
    }
}

void receiver::heap_ready(const spead2::recv::heap &heap)
{
    switch (state)
    {
    case state_t::METADATA:
        process_metadata(heap);
        break;
    case state_t::DATA:
        process_data(heap);
        break;
    case state_t::STOP:
        break;
    }
}

void receiver::stop_received()
{
    if (state == state_t::DATA)
    {
        try
        {
            flush_all();
        }
        catch (spead2::ringbuffer_stopped)
        {
            // can get here is we were called via receiver::stop
        }
    }
    ring.stop();
    state = state_t::STOP;
}

void receiver::graceful_stop()
{
    stream.get_strand().post([this] { stop_received(); });
}

void receiver::stop()
{
    /* Stop the ring first, so that we unblock the internals if they
     * are waiting for space in ring.
     */
    ring.stop();
    stream.stop();
}

receiver::receiver(const session_config &config)
    : window<slice, receiver>(window_size),
    config(config),
    ticks_between_spectra(config.ticks_between_spectra),
    channels(config.channels),
    spectra_per_heap(config.spectra_per_heap),
    channels_per_heap(config.channels_per_heap),
    worker(1, affinity_vector(config.network_affinity)),
    stream(*this, std::max(1, config.channels / config.channels_per_heap) * config.live_heaps_per_substream),
    ring(config.ring_slots),
    free_ring(window_size + config.ring_slots + 1)
{
    py::gil_scoped_release gil;

    try
    {
        if (config.ibv)
        {
            use_ibv = true;
#if !SPEAD2_USE_IBV
            logger(spead2::log_level::warning, "Not using ibverbs because support is not compiled in");
            use_ibv = false;
#endif
            if (use_ibv)
            {
                for (const auto &endpoint : config.endpoints)
                    if (!endpoint.address().is_multicast())
                    {
                        log_format(spead2::log_level::warning, "Not using ibverbs because endpoint %1% is not multicast",
                               endpoint);
                        use_ibv = false;
                        break;
                    }
            }
            if (use_ibv && config.interface_address.is_unspecified())
            {
                logger(spead2::log_level::warning, "Not using ibverbs because interface address is not specified");
                use_ibv = false;
            }
        }

        if (metadata_ready())
            prepare_for_data();

        emplace_readers();
    }
    catch (std::exception)
    {
        /* Normally we can rely on the destructor to call stop() (which is
         * necessary to ensure that the stream isn't going to make more calls
         * into the receiver while it is being destroyed), but an exception
         * thrown from the constructor does not cause the destructor to get
         * called.
         */
        stop();
        throw;
    }
}

receiver::~receiver()
{
    stop();
}


class session
{
private:
    const session_config config;
    receiver recv;
    std::future<void> run_future;
    std::int64_t n_heaps = 0;
    std::int64_t n_total_heaps = 0;

    void run_impl();  // internal implementation of run
    void run();       // runs in a separate thread

public:
    explicit session(const session_config &config);
    ~session();

    void join();
    void stop_stream();

    std::int64_t get_n_heaps() const;
    std::int64_t get_n_total_heaps() const;
    std::int64_t get_first_timestamp() const;
};

session::session(const session_config &config) :
    config(config),
    recv(config),
    run_future(std::async(std::launch::async, &session::run, this))
{
}

session::~session()
{
    py::gil_scoped_release gil;
    recv.stop();
    if (run_future.valid())
        run_future.wait();   // don't get(), since that could throw
}

void session::join()
{
    py::gil_scoped_release gil;
    if (run_future.valid())
    {
        run_future.wait();
        // The run function should have done this, but if it exited by
        // exception then it won't. It's idempotent, so call it again
        // to be sure.
        recv.stop();
        run_future.get(); // this can throw an exception
    }
}

void session::stop_stream()
{
    py::gil_scoped_release gil;
    recv.graceful_stop();
}

void session::run()
{
    try
    {
        //H5::Exception::dontPrint();
        run_impl();
    }
    catch (H5::Exception &e)
    {
        throw std::runtime_error(e.getFuncName() + ": " + e.getDetailMsg());
    }
}

void session::run_impl()
{
    if (config.disk_affinity >= 0)
        spead2::thread_pool::set_affinity(config.disk_affinity);

    spead2::ringbuffer<slice> &ring = recv.ring;
    spead2::ringbuffer<slice> &free_ring = recv.free_ring;
    // Wait for the metadata
    try
    {
        ring.pop();
    }
    catch (spead2::ringbuffer_stopped)
    {
        recv.stop();
        return;   // stream stopped before we saw the metadata
    }

    int channels = recv.get_channels();
    int channels_per_heap = recv.get_channels_per_heap();
    int spectra_per_heap = recv.get_spectra_per_heap();
    std::int64_t ticks_between_spectra = recv.get_ticks_between_spectra();

    hdf5_writer w(config.filename, config.direct,
                  channels, channels_per_heap, spectra_per_heap, ticks_between_spectra);
    int fd = w.get_fd();
    struct statfs stat;
    if (fstatfs(fd, &stat) < 0)
        throw std::system_error(errno, std::system_category(), "fstatfs failed");
    std::size_t slice_size = 2 * spectra_per_heap * channels;
    std::size_t reserve_blocks = (1024 * 1024 * 1024 + 1000 * slice_size) / stat.f_bsize;

    boost::format progress_formatter("dropped %1% of %2%");
    bool done = false;
    // Number of heaps in time between disk space checks
    constexpr std::int64_t check_cadence = 1000;
    // When time_heaps passes this value, we check disk space and log a message
    std::int64_t next_check = check_cadence;
    while (!done)
    {
        try
        {
            slice s = ring.pop();
            n_heaps += s.n_present;
            w.add(s);
            std::int64_t time_heaps = (s.spectrum + spectra_per_heap) / spectra_per_heap;
            std::int64_t total_heaps = time_heaps * (channels / channels_per_heap);
            if (total_heaps > n_total_heaps)
            {
                n_total_heaps = total_heaps;
                if (time_heaps >= next_check)
                {
                    progress_formatter % (n_total_heaps - n_heaps) % n_total_heaps;
                    logger(spead2::log_level::info, progress_formatter.str());
                    if (fstatfs(fd, &stat) < 0)
                        throw std::system_error(errno, std::system_category(), "fstatfs failed");
                    if (stat.f_bavail < reserve_blocks)
                    {
                        logger(spead2::log_level::info, "stopping capture due to lack of free space");
                        done = true;
                    }
                    // Find next multiple of check_cadence strictly greater
                    // than time_heaps.
                    next_check = (time_heaps / check_cadence + 1) * check_cadence;
                }
            }
            free_ring.push(std::move(s));
        }
        catch (spead2::ringbuffer_stopped &e)
        {
            done = true;
        }
    }
    recv.stop();
}

std::int64_t session::get_n_heaps() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve n_heaps while running");
    return n_heaps;
}

std::int64_t session::get_n_total_heaps() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve n_total_heaps while running");
    return n_total_heaps;
}

std::int64_t session::get_first_timestamp() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve first_timestamp while running");
    return recv.get_first_timestamp();
}

PYBIND11_PLUGIN(_bf_ingest_session)
{
    using namespace pybind11::literals;
    py::module m("_bf_ingest_session", "C++ backend of beamformer capture");

    py::class_<session_config>(m, "SessionConfig", "Configuration data for the backend")
        .def(py::init<const std::string &>(), "filename"_a)
        .def_readwrite("filename", &session_config::filename)
        .def_property("interface_address", &session_config::get_interface_address, &session_config::set_interface_address)
        .def_readwrite("buffer_size", &session_config::buffer_size)
        .def_readwrite("live_heaps_per_substream", &session_config::live_heaps_per_substream)
        .def_readwrite("ring_slots", &session_config::ring_slots)
        .def_readwrite("ibv", &session_config::ibv)
        .def_readwrite("comp_vector", &session_config::comp_vector)
        .def_readwrite("network_affinity", &session_config::network_affinity)
        .def_readwrite("disk_affinity", &session_config::disk_affinity)
        .def_readwrite("direct", &session_config::direct)
        .def_readwrite("ticks_between_spectra", &session_config::ticks_between_spectra)
        .def_readwrite("channels", &session_config::channels)
        .def_readwrite("spectra_per_heap", &session_config::spectra_per_heap)
        .def_readwrite("channels_per_heap", &session_config::channels_per_heap)
        .def_readwrite("spead_metadata", &session_config::spead_metadata)
        .def("add_endpoint", &session_config::add_endpoint, "bind_host"_a, "port"_a);
    ;
    py::class_<session>(m, "Session", "Capture session")
        .def(py::init<const session_config &>(), "config"_a)
        .def("join", &session::join)
        .def("stop_stream", &session::stop_stream)
        .def_property_readonly("n_heaps", &session::get_n_heaps)
        .def_property_readonly("n_total_heaps", &session::get_n_total_heaps)
        .def_property_readonly("first_timestamp", &session::get_first_timestamp)
    ;

    py::object logging_module = py::module::import("logging");
    py::object spead2_logger = logging_module.attr("getLogger")("spead2");
    py::object my_logger = logging_module.attr("getLogger")("katsdpingest.bf_ingest_session");
    spead2::set_log_function(log_function_python(spead2_logger));
    logger = log_function_python(my_logger);

    return m.ptr();
}

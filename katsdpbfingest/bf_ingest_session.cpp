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
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include <spead2/recv_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/recv_ring_stream.h>
#include <spead2/recv_udp_ibv.h>
#include <spead2/recv_udp.h>
#include <spead2/common_memory_pool.h>
#include <spead2/common_thread_pool.h>
#include <spead2/py_common.h>
#include <sys/mman.h>
#include <sys/vfs.h>
#include <system_error>
#include <cerrno>
#include <cstdlib>
#include <H5Cpp.h>
#include <boost/python.hpp>

namespace py = boost::python;

static constexpr int ALIGNMENT = 4096;
static spead2::log_function_python logger;

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
static std::unique_ptr<T[], free_delete<T>> make_aligned(std::size_t elements)
{
    void *ptr = aligned_alloc(ALIGNMENT, elements * sizeof(T));
    if (!ptr)
        throw std::bad_alloc();
    return std::unique_ptr<T[], free_delete<T>>(static_cast<T*>(ptr));
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

class hdf5_bf_raw_writer
{
private:
    const int channels;
    const int spectra_per_dump;
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;

public:
    hdf5_bf_raw_writer(H5::CommonFG &parent, int channels, int spectra_per_dump,
                       const char *name);

    void add(std::uint8_t *ptr, std::size_t length, std::uint64_t dump_idx);
};

hdf5_bf_raw_writer::hdf5_bf_raw_writer(H5::CommonFG &parent, int channels, int spectra_per_dump,
                                       const char *name)
    : channels(channels), spectra_per_dump(spectra_per_dump),
    dxpl(make_dxpl_direct(channels * spectra_per_dump * 2))
{
    hsize_t dims[3] = {hsize_t(channels), 0, 2};
    hsize_t maxdims[3] = {hsize_t(channels), H5S_UNLIMITED, 2};
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_dump), 2};
    H5::DataSpace file_space(3, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(3, chunk);
    dataset = parent.createDataSet(name, H5::PredType::STD_I8BE, file_space, dcpl);
}

void hdf5_bf_raw_writer::add(std::uint8_t *ptr, std::size_t length, std::uint64_t dump_idx)
{
    assert(length == 2U * channels * spectra_per_dump);
    hssize_t time_idx = dump_idx * spectra_per_dump;
    hsize_t new_size[3] = {hsize_t(channels), hsize_t(time_idx) + spectra_per_dump, 2};
    dataset.extend(new_size);
    const hsize_t offset[3] = {0, hsize_t(time_idx), 0};
    const hsize_t *offset_ptr = offset;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
    dataset.write(ptr, H5::PredType::STD_I8BE, H5::DataSpace::ALL, H5::DataSpace::ALL, dxpl);
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
    const int spectra_per_dump;
    const std::uint64_t timestamp_step;  ///< Timestamp difference between spectra

    hdf5_timestamps_writer(H5::CommonFG &parent, int spectra_per_dump,
                           std::uint64_t timestamp_step, const char *name);
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
    H5::CommonFG &parent, int spectra_per_dump,
    std::uint64_t timestamp_step, const char *name)
    : dxpl(make_dxpl_direct(chunk * sizeof(std::uint64_t))),
    spectra_per_dump(spectra_per_dump),
    timestamp_step(timestamp_step)
{
    hsize_t dims[1] = {0};
    hsize_t maxdims[1] = {H5S_UNLIMITED};
    H5::DataSpace file_space(1, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(1, &chunk);
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
    for (int i = 0; i < spectra_per_dump; i++)
    {
        buffer[n_buffer++] = timestamp;
        timestamp += timestamp_step;
    }
    assert(n_buffer <= chunk);
    if (n_buffer == chunk)
        flush();
}

class hdf5_writer
{
private:
    std::int64_t first_timestamp = -1;
    std::uint64_t past_end_timestamp = 0;
    H5::H5File file;
    H5::Group group;
    hdf5_bf_raw_writer bf_raw;
    hdf5_timestamps_writer captured_timestamps, all_timestamps;

    static H5::FileAccPropList make_fapl(bool direct);

public:
    hdf5_writer(const std::string &filename, bool direct,
                int channels, int spectra_per_dump, std::uint64_t timestamp_step)
        : file(filename, H5F_ACC_TRUNC, H5::FileCreatPropList::DEFAULT, make_fapl(direct)),
        group(file.createGroup("Data")),
        bf_raw(group, channels, spectra_per_dump, "bf_raw"),
        captured_timestamps(group, spectra_per_dump, timestamp_step, "captured_timestamps"),
        all_timestamps(group, spectra_per_dump, timestamp_step, "timestamps")
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
        const std::int32_t version = 2;
        version_attr.write(H5::PredType::NATIVE_INT32, &version);
    }

    void add(std::uint8_t *ptr, std::size_t length,
             std::uint64_t timestamp, std::uint64_t dump_idx,
             spead2::recv::heap &&heap);

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
    // Older version of libhdf5 are missing the C++ version setLibverBounds
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

void hdf5_writer::add(std::uint8_t *ptr, std::size_t length,
                      std::uint64_t timestamp, std::uint64_t dump_idx,
                      spead2::recv::heap &&heap)
{
    if (first_timestamp == -1)
    {
        first_timestamp = timestamp;
        past_end_timestamp = timestamp;
    }
    while (past_end_timestamp <= timestamp)
    {
        all_timestamps.add(past_end_timestamp);
        past_end_timestamp += all_timestamps.timestamp_step * all_timestamps.spectra_per_dump;
    }
    captured_timestamps.add(timestamp);
    bf_raw.add(ptr, length, dump_idx);
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
    int live_heaps = 2;
    int ring_heaps = 128;
    bool ibv = false;
    int comp_vector = 0;
    int network_affinity = -1;

    int disk_affinity = -1;
    bool direct = false;

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


class session
{
private:
    const session_config config;
    spead2::thread_pool worker;
    spead2::recv::ring_stream<> stream;
    std::future<void> run_future;
    std::uint64_t n_dumps = 0;
    std::uint64_t n_total_dumps = 0;
    std::int64_t first_timestamp = -1;

    static std::vector<int> affinity_vector(int affinity);

    void run_impl();  // internal implementation of run
    void run();       // runs in a separate thread

public:
    explicit session(const session_config &config);
    ~session() { stream.stop(); }

    void join();
    void stop_stream();

    std::uint64_t get_n_dumps() const;
    std::uint64_t get_n_total_dumps() const;
    std::int64_t get_first_timestamp() const;
};

std::vector<int> session::affinity_vector(int affinity)
{
    if (affinity < 0)
        return {};
    else
        return {affinity};
}

session::session(const session_config &config) :
    config(config),
    worker(1, affinity_vector(config.network_affinity)),
    stream(worker, 0, config.live_heaps, config.ring_heaps),
    run_future(std::async(std::launch::async, &session::run, this))
{
}

void session::join()
{
    spead2::release_gil gil;
    if (run_future.valid())
    {
        run_future.wait();
        // The run function should have done this, but if it exited by
        // exception then it won't. It's idempotent, so call it again
        // to be sure.
        stream.stop();
        run_future.get(); // this can throw an exception
    }
}

void session::stop_stream()
{
    spead2::release_gil gil;
    stream.stop();
}

// Parse the shape from either the shape field or the numpy header
static std::vector<spead2::s_item_pointer_t> get_shape(const spead2::descriptor &descriptor)
{
    using spead2::s_item_pointer_t;

    if (!descriptor.numpy_header.empty())
    {
        // Slightly hacky approach to find out the shape (without
        // trying to implement a Python interpreter)
        boost::regex expr("['\"]shape['\"]:\\s*\\(([^)]*)\\)");
        boost::smatch what;
        if (regex_search(descriptor.numpy_header, what, expr, boost::match_extra))
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
    std::shared_ptr<spead2::memory_allocator> allocator = std::make_shared<spead2::mmap_allocator>();
    /* We need to set this before adding the reader, so that any data heaps
     * that are successfully captured prior to the memory pool being set are
     * correctly aligned for O_DIRECT.
     */
    stream.set_memory_allocator(allocator);
    stream.set_memcpy(spead2::MEMCPY_NONTEMPORAL);
    bool use_ibv = true;
    if (config.ibv)
    {
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

    // Wait for metadata
    int channels = -1;
    int spectra_per_dump = -1;
    int ticks_between_spectra = -1;
    int bf_raw_id = -1;
    int timestamp_id = -1;
    int ticks_between_spectra_id = -1;
    while (channels == -1 || spectra_per_dump == -1 || ticks_between_spectra == -1
           || bf_raw_id == -1 || timestamp_id == -1)
    {
        try
        {
            spead2::recv::heap fh = stream.pop();
            for (const auto &descriptor : fh.get_descriptors())
            {
                if (descriptor.name == "bf_raw")
                {
                    auto shape = get_shape(descriptor);
                    if (shape.size() == 3 && shape[2] == 2)
                    {
                        channels = shape[0];
                        spectra_per_dump = shape[1];
                    }
                    bf_raw_id = descriptor.id;
                }
                else if (descriptor.name == "timestamp")
                    timestamp_id = descriptor.id;
                else if (descriptor.name == "ticks_between_spectra")
                    ticks_between_spectra_id = descriptor.id;
            }
            for (const auto item : fh.get_items())
                if (item.id == ticks_between_spectra_id)
                    ticks_between_spectra = item.immediate_value;
        }
        catch (spead2::ringbuffer_stopped &e)
        {
            logger(spead2::log_level::warning, "stream stopped before we received metadata");
            return;
        }
    }
    logger(spead2::log_level::info, "metadata received");
    const std::size_t payload_size = channels * spectra_per_dump * 2;
    const std::uint64_t dump_step = std::uint64_t(ticks_between_spectra) * spectra_per_dump;

    /* We size the memory pool so that it should never run out. For this, we
     * need slots for
     * - live heaps
     * - heaps in the ringbuffer
     * - one heap being expelled from live heaps but blocked on the ringbuffer
     * - one heap being written to disk
     * - one extra just in case I forgot something
     */
    int mp_slots = config.live_heaps + config.ring_heaps + 3;
    std::shared_ptr<spead2::memory_pool> pool = std::make_shared<spead2::memory_pool>(
        0, payload_size, mp_slots, mp_slots, allocator);
    stream.set_memory_allocator(pool);

    std::int64_t first_timestamp = -1;
    hdf5_writer w(config.filename, config.direct, channels, spectra_per_dump, ticks_between_spectra);
    int fd = w.get_fd();
    struct statfs stat;
    if (fstatfs(fd, &stat) < 0)
        throw std::system_error(errno, std::system_category(), "fstatfs failed");
    std::size_t reserve_blocks = (1024 * 1024 * 1024 + 1000 * payload_size) / stat.f_bsize;

    boost::format progress_formatter("dropped %1% of %2%");
    while (true)
    {
        try
        {
            spead2::recv::heap fh = stream.pop();
            const auto &items = fh.get_items();
            std::int64_t timestamp = -1;
            const spead2::recv::item *data_item = nullptr;

            for (const auto &item : items)
            {
                if (item.id == timestamp_id)
                    timestamp = item.immediate_value;
                else if (item.id == bf_raw_id)
                    data_item = &item;
            }
            if (data_item != nullptr && timestamp != -1)
            {
                if (first_timestamp == -1)
                    first_timestamp = timestamp;
                if (timestamp < first_timestamp)
                {
                    log_format(spead2::log_level::warning, "timestamp %1% pre-dates start %2%, discarding",
                               timestamp, first_timestamp);
                    continue;
                }
                if ((timestamp - first_timestamp) % dump_step != 0)
                {
                    log_format(spead2::log_level::warning, "timestamp %1% is not properly aligned to %2%, discarding",
                               timestamp, dump_step);
                    continue;
                }
                if (data_item->length != payload_size)
                {
                    log_format(spead2::log_level::warning, "bf_raw item has wrong length (%1% != %2%), discarding",
                               data_item->length, payload_size);
                    continue;
                }
                n_dumps++;
                std::uint64_t dump_idx = (timestamp - first_timestamp) / dump_step;
                n_total_dumps = std::max(n_total_dumps, dump_idx + 1);
                w.add(data_item->ptr, data_item->length, timestamp, dump_idx, std::move(fh));
                if (n_total_dumps % 1000 == 0)
                {
                    progress_formatter % (n_total_dumps - n_dumps) % n_total_dumps;
                    logger(spead2::log_level::info, progress_formatter.str());
                    if (fstatfs(fd, &stat) < 0)
                        throw std::system_error(errno, std::system_category(), "fstatfs failed");
                    if (stat.f_bavail < reserve_blocks)
                    {
                        logger(spead2::log_level::info, "stopping capture due to lack of free space");
                        break;
                    }
                }
            }
        }
        catch (spead2::ringbuffer_stopped &e)
        {
            break;
        }
    }
    stream.stop();
}

std::uint64_t session::get_n_dumps() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve n_dumps while running");
    return n_dumps;
}

std::uint64_t session::get_n_total_dumps() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve n_total_dumps while running");
    return n_total_dumps;
}

std::int64_t session::get_first_timestamp() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve n_dumps while running");
    return first_timestamp;
}

BOOST_PYTHON_MODULE(_bf_ingest_session)
{
    using namespace boost::python;

    class_<session_config>("SessionConfig",
        init<const std::string &>(arg("filename")))
        .def_readwrite("filename", &session_config::filename)
        .add_property("interface_address", &session_config::get_interface_address, &session_config::set_interface_address)
        .def_readwrite("buffer_size", &session_config::buffer_size)
        .def_readwrite("live_heaps", &session_config::live_heaps)
        .def_readwrite("ring_heaps", &session_config::ring_heaps)
        .def_readwrite("ibv", &session_config::ibv)
        .def_readwrite("comp_vector", &session_config::comp_vector)
        .def_readwrite("network_affinity", &session_config::network_affinity)
        .def_readwrite("disk_affinity", &session_config::disk_affinity)
        .def_readwrite("direct", &session_config::direct)
        .def("add_endpoint", &session_config::add_endpoint,
             (arg("bind_host"), arg("port")));
    ;
    class_<session, boost::noncopyable>("Session",
        init<const session_config &>(arg("config")))
        .def("join", &session::join)
        .def("stop_stream", &session::stop_stream)
        .add_property("n_dumps", &session::get_n_dumps)
        .add_property("n_total_dumps", &session::get_n_total_dumps)
        .add_property("first_timestamp", &session::get_first_timestamp)
    ;

    object logging_module = import("logging");
    object spead2_logger = logging_module.attr("getLogger")("spead2");
    object my_logger = logging_module.attr("getLogger")("katsdpingest.bf_ingest_session");
    spead2::set_log_function(spead2::log_function_python(spead2_logger));
    logger = spead2::log_function_python(my_logger);
}

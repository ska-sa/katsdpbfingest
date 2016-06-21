/* Still TODO:
 * - record more stats and expose them
 * - gracefully handle libhdf5 exceptions
 * - grow the file in batches, shrink again at end
 * - set the version attribute on the file
 * - set the attributes on the timestamp datasets
 * - finalise file on capture-stop instead of on capture-done
 *   (might require using a Python thread instead of a C++ thread)
 * - change --cbf-spead command-line option to be a single endpoint (or accept multiple)
 * - remove --buffer command-line option
 * - add --direct command-line option
 * - switch back to using ibv_verbs, make interface_address optional
 */

#include <memory>
#include <algorithm>
#include <vector>
#include <string>
#include <sstream>
#include <unistd.h>
#include <boost/lexical_cast.hpp>
#include <boost/regex.hpp>
#include "recv_stream.h"
#include "recv_heap.h"
#include "recv_ring_stream.h"
#include "recv_udp_ibv.h"
#include "recv_udp.h"
#include "common_memory_pool.h"
#include "common_thread_pool.h"
#include <sys/mman.h>
#include <system_error>
#include <cstdlib>
#include <H5Cpp.h>
#include <boost/python.hpp>

namespace py = boost::python;

constexpr int ALIGNMENT = 4096;

// Suppresses all spead2 logging
struct logger
{
    void operator()(spead2::log_level, const std::string &) {}
};

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

class hdf5_bf_raw_writer
{
private:
    const int channels;
    const int spectra_per_dump;
    H5::DataSpace file_space;
    H5::DataSpace memory_space;
    H5::DataSet dataset;

public:
    hdf5_bf_raw_writer(H5::CommonFG &parent, int channels, int spectra_per_dump,
                       const char *name);

    void add(std::uint8_t *ptr, std::size_t length, std::uint64_t dump_idx);
};

hdf5_bf_raw_writer::hdf5_bf_raw_writer(H5::CommonFG &parent, int channels, int spectra_per_dump,
                                       const char *name)
    : channels(channels), spectra_per_dump(spectra_per_dump)
{
    hsize_t dims[3] = {hsize_t(channels), 0, 2};
    hsize_t maxdims[3] = {hsize_t(channels), H5S_UNLIMITED, 2};
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_dump), 2};
    file_space = H5::DataSpace(3, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(3, chunk);
    dataset = parent.createDataSet(name, H5::PredType::STD_I8BE, file_space, dcpl);
    memory_space = H5::DataSpace(3, chunk);
    memory_space.selectAll();
}

void hdf5_bf_raw_writer::add(std::uint8_t *ptr, std::size_t length, std::uint64_t dump_idx)
{
    assert(length == 2U * channels * spectra_per_dump);
    hssize_t time_idx = dump_idx * spectra_per_dump;
    hsize_t new_size[3] = {hsize_t(channels), hsize_t(time_idx) + spectra_per_dump, 2};
    dataset.extend(new_size);
    dataset.getSpace().extentCopy(file_space);
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_dump), 2};
    hsize_t start[3] = {0, hsize_t(time_idx), 0};
    file_space.selectHyperslab(H5S_SELECT_SET, chunk, start);
    dataset.write(ptr, H5::PredType::STD_I8BE, memory_space, file_space);
}

class hdf5_timestamps_writer
{
private:
    static constexpr hsize_t chunk = 1048576;
    H5::DataSpace file_space;
    H5::DataSpace memory_space;
    H5::DataSet dataset;
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

hdf5_timestamps_writer::hdf5_timestamps_writer(
    H5::CommonFG &parent, int spectra_per_dump,
    std::uint64_t timestamp_step, const char *name)
    : spectra_per_dump(spectra_per_dump),
    timestamp_step(timestamp_step)
{
    hsize_t dims[1] = {0};
    hsize_t maxdims[1] = {H5S_UNLIMITED};
    file_space = H5::DataSpace(1, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(1, &chunk);
    dataset = parent.createDataSet(
        name, H5::PredType::NATIVE_UINT64, file_space, dcpl);
    buffer = make_aligned<std::uint64_t>(chunk);
    n_buffer = 0;
    memory_space = H5::DataSpace(1, &chunk);
}

hdf5_timestamps_writer::~hdf5_timestamps_writer()
{
    if (n_buffer > 0)
        flush();
}

void hdf5_timestamps_writer::flush()
{
    hsize_t new_size = n_written + n_buffer;
    dataset.extend(&new_size);
    dataset.getSpace().extentCopy(file_space);
    file_space.selectHyperslab(H5S_SELECT_SET, &n_buffer, &n_written);
    if (n_buffer == chunk)
    {
        // This is the common space, and I'm just guessing that it might
        // be faster. The else branch would still be valid.
        memory_space.selectAll();
    }
    else
    {
        hsize_t start = 0;
        memory_space.selectHyperslab(H5S_SELECT_SET, &n_buffer, &start);
    }
    dataset.write(
        buffer.get(),
        H5::PredType::NATIVE_UINT64,
        memory_space, file_space);

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
    }

    void add(std::uint8_t *ptr, std::size_t length,
             std::uint64_t timestamp, std::uint64_t dump_idx,
             spead2::recv::heap &&heap);
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


struct session_config
{
    std::string filename;
    boost::asio::ip::udp::endpoint multicast;
    boost::asio::ip::address interface_address;
    int total_channels = 4096;

    std::size_t buffer_size = 16 * 1024 * 1024;
    int live_heaps = 2;
    int ring_heaps = 64;
    int comp_vector = 0;
    int network_affinity = -1;

    int disk_affinity = -1;
    bool direct = false;

    session_config(const std::string &filename, const std::string &multicast_group, int port,
                   const std::string interface_address);
};

session_config::session_config(const std::string &filename, const std::string &multicast_group, int port,
                               const std::string interface_address)
    : filename(filename),
    multicast(boost::asio::ip::address_v4::from_string(multicast_group), port),
    interface_address(boost::asio::ip::address_v4::from_string(interface_address))
{
}

class session
{
private:
    const session_config config;
    spead2::thread_pool worker;
    spead2::recv::ring_stream<> stream;
    std::thread run_thread;
    std::uint64_t n_dumps = 0;
    std::int64_t first_timestamp = -1;

    static std::vector<int> affinity_vector(int affinity);

    void run(); // runs in a separate thread

public:
    explicit session(const session_config &config);
    ~session() { stop(); }

    void join();
    void stop();

    std::uint64_t get_n_dumps() const;
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
    run_thread(std::bind(&session::run, this))
{
}

void session::join()
{
    if (run_thread.joinable())
        run_thread.join();
}

void session::stop()
{
    stream.stop();
    join();
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
    if (config.disk_affinity >= 0)
        spead2::thread_pool::set_affinity(config.disk_affinity);
    std::shared_ptr<spead2::memory_allocator> allocator = std::make_shared<spead2::mmap_allocator>();
    stream.set_memcpy(spead2::MEMCPY_NONTEMPORAL);
    stream.emplace_reader<spead2::recv::udp_reader>(
        config.multicast, spead2::recv::udp_reader::default_max_size, config.buffer_size);

    // Wait for metadata
    int channels = -1;
    int spectra_per_dump = -1;
    int bf_raw_id = -1;
    int timestamp_id = -1;
    while (channels == -1 || spectra_per_dump == -1 || bf_raw_id == -1 || timestamp_id == -1)
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
            }
        }
        catch (spead2::ringbuffer_stopped &e)
        {
            std::cerr << "Stream stopped before we received metadata\n";
            return;
        }
    }
    std::cout << "Metadata received\n";
    const std::size_t payload_size = channels * spectra_per_dump * 2;
    const std::uint64_t dump_step = 2 * config.total_channels * spectra_per_dump;

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
    stream.set_memory_pool(pool);

    std::int64_t first_timestamp = -1;
    hdf5_writer w(config.filename, config.direct, channels, spectra_per_dump, 2 * config.total_channels);

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
                    std::cerr << "timestamp pre-dates start, discarding\n";
                    continue;
                }
                if ((timestamp - first_timestamp) % dump_step != 0)
                {
                    std::cerr << "timestamp is not properly aligned, discarding\n";
                    continue;
                }
                n_dumps++;
                std::uint64_t dump_idx = (timestamp - first_timestamp) / dump_step;
                if (n_dumps % 1024 == 0)
                    std::cout << "Received " << n_dumps << '/' << dump_idx + 1 << " (" << dump_idx + 1 - n_dumps << " dropped)" << std::endl;
                if (data_item->length != payload_size)
                    std::cerr << "size mismatch: " << data_item->length
                        << " != " << payload_size << '\n';
                w.add(data_item->ptr, data_item->length, timestamp, dump_idx, std::move(fh));
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
    if (run_thread.joinable())
        throw std::runtime_error("cannot retrieve n_dumps while running");
    return n_dumps;
}

std::int64_t session::get_first_timestamp() const
{
    if (run_thread.joinable())
        throw std::runtime_error("cannot retrieve n_dumps while running");
    return first_timestamp;
}

BOOST_PYTHON_MODULE(_bf_ingest_session)
{
    using namespace boost::python;

    class_<session_config>("SessionConfig",
        init<const std::string &, const std::string &, int, const std::string &>(
            (arg("filename"), arg("multicast_group"), arg("port"), arg("interface_address"))))
        .def_readwrite("filename", &session_config::filename)
        .def_readwrite("total_channels", &session_config::total_channels)
        .def_readwrite("buffer_size", &session_config::buffer_size)
        .def_readwrite("live_heaps", &session_config::live_heaps)
        .def_readwrite("ring_heaps", &session_config::ring_heaps)
        .def_readwrite("comp_vector", &session_config::comp_vector)
        .def_readwrite("network_affinity", &session_config::network_affinity)
        .def_readwrite("disk_affinity", &session_config::disk_affinity)
        .def_readwrite("direct", &session_config::direct)
    ;
    class_<session, boost::noncopyable>("Session",
        init<const session_config &>(arg("config")))
        .def("join", &session::join)
        .def("stop", &session::stop)
        .def_readonly("n_dumps", &session::get_n_dumps)
        .def_readonly("first_timestamp", &session::get_first_timestamp)
    ;
}

#include <cstdint>
#include <cstddef>
#include <future>
#include <system_error>
#include <sys/statfs.h>
#include <H5Cpp.h>
#include <spead2/common_ringbuffer.h>
#include <spead2/common_thread_pool.h>
#include <pybind11/pybind11.h>
#include "common.h"
#include "session.h"
#include "writer.h"
#include "stats.h"

// TODO: only used for gil_scoped_release. Would be nice to find a way to avoid
// having this file depend on pybind11.
namespace py = pybind11;

session::session(const session_config &config) :
    config(config.validate()),
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

    auto ring = recv.stream.get_data_ringbuffer();

    const unit_system<std::int64_t, units::bytes, units::spectra, units::heaps::time, units::slices::time> time_sys(
        2 * sizeof(std::int8_t), config.spectra_per_heap,
        config.heaps_per_slice_time);
    const unit_system<std::int64_t, units::channels, units::heaps::freq, units::slices::freq> freq_sys(
        config.channels_per_heap, config.channels / config.channels_per_heap);

    std::unique_ptr<stats_collector> stats;
    if (!config.stats_endpoint.address().is_unspecified())
    {
        stats.reset(new stats_collector(config));
    }

    std::unique_ptr<hdf5_writer> w;
    int fd = -1;
    std::size_t reserve_blocks = 0;
    q::heaps n_total_heaps{0};
    struct statfs stat;
    // Number of heaps in time between disk space checks
    constexpr q::heaps_t check_cadence{1000};
    if (config.filename)
    {
        w.reset(new hdf5_writer(*config.filename, config.direct,
                                config.get_freq_system(), config.get_time_system()));
        fd = w->get_fd();
        if (fstatfs(fd, &stat) < 0)
            throw std::system_error(errno, std::system_category(), "fstatfs failed");
        const q::bytes check_bytes =
            freq_sys.scale_factor<units::slices::freq, units::channels>()
            * time_sys.convert<units::bytes>(check_cadence);
        reserve_blocks = (1024 * 1024 * 1024 + check_bytes.get()) / stat.f_bsize;
        log_format(spead2::log_level::info, "capture will stop when disk space is %d bytes",
                   reserve_blocks * stat.f_bsize);
    }

    bool done = false;
    // When time_heaps passes this value, we check disk space and log a message
    q::heaps_t next_check = check_cadence;
    while (!done)
    {
        try
        {
            std::unique_ptr<slice> s(static_cast<slice *>(ring->pop().release()));
            if (stats)
                stats->add(*s);
            if (w)
                w->add(*s);
            q::heaps_t time_heaps = time_sys.convert_down<units::heaps::time>(s->spectrum) + q::heaps_t(1);
            q::heaps total_heaps = time_heaps * freq_sys.convert_one<units::slices::freq, units::heaps::freq>();
            if (total_heaps > n_total_heaps)
            {
                n_total_heaps = total_heaps;
                if (time_heaps >= next_check)
                {
                    if (w)
                    {
                        if (fstatfs(fd, &stat) < 0)
                            throw std::system_error(errno, std::system_category(), "fstatfs failed");
                        if (stat.f_bavail < reserve_blocks)
                        {
                            log_format(spead2::log_level::info, "stopping capture due to lack of free space: have %d blocks of %d, need %d",
                                       stat.f_bavail, stat.f_bsize, reserve_blocks);
                            done = true;
                        }
                    }
                    // Find next multiple of check_cadence strictly greater
                    // than time_heaps.
                    next_check = (time_heaps / check_cadence + 1) * check_cadence;
                }
            }
            recv.stream.add_free_chunk(std::move(s));
        }
        catch (spead2::ringbuffer_stopped &e)
        {
            done = true;
        }
    }
    recv.stop();
}

spead2::recv::stream_stats session::get_counters() const
{
    return recv.stream.get_stats();
}

std::int64_t session::get_first_timestamp() const
{
    if (run_future.valid())
        throw std::runtime_error("cannot retrieve first_timestamp while running");
    return recv.get_first_timestamp().get();
}

void session::add_tcp_reader(const spead2::socket_wrapper<boost::asio::ip::tcp::acceptor> &acceptor)
{
    recv.add_tcp_reader(acceptor);
}

#include <cstdint>
#include <cstddef>
#include <future>
#include <system_error>
#include <sys/statfs.h>
#include <H5Cpp.h>
#include <boost/format.hpp>
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

    int channels = config.channels;
    int channels_per_heap = config.channels_per_heap;
    int spectra_per_heap = config.spectra_per_heap;
    std::int64_t ticks_between_spectra = config.ticks_between_spectra;

    std::unique_ptr<stats_collector> stats;
    if (!config.stats_endpoint.address().is_unspecified())
    {
        stats.reset(new stats_collector(config.stats_endpoint, config.stats_interface_address,
                                        channels, spectra_per_heap));
    }

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
            if (stats)
                stats->add(s);
            w.add(s);
            std::int64_t time_heaps = (s.spectrum + spectra_per_heap) / spectra_per_heap;
            std::int64_t total_heaps = time_heaps * (channels / channels_per_heap);
            if (total_heaps > n_total_heaps)
            {
                n_total_heaps = total_heaps;
                if (time_heaps >= next_check)
                {
                    progress_formatter % (n_total_heaps - n_heaps) % n_total_heaps;
                    log_message(spead2::log_level::info, progress_formatter.str());
                    if (fstatfs(fd, &stat) < 0)
                        throw std::system_error(errno, std::system_category(), "fstatfs failed");
                    if (stat.f_bavail < reserve_blocks)
                    {
                        log_message(spead2::log_level::info, "stopping capture due to lack of free space");
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



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
#include <string>
#include <functional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <spead2/common_logging.h>
#include <spead2/py_common.h>
#include "common.h"
#include "session.h"

/* Work around pybind11 not supporting experimental::optional
 * if <optional> exists. Needed for GCC 7.3 at least.
 */
#if !PYBIND11_HAS_EXP_OPTIONAL
namespace pybind11
{
namespace detail
{
template<typename T> struct type_caster<std::experimental::optional<T>>
    : public optional_caster<std::experimental::optional<T>> {};
}
}
#endif

namespace py = pybind11;

static std::unique_ptr<spead2::log_function_python> spead2_logger;

PYBIND11_MODULE(_bf_ingest, m)
{
    using namespace pybind11::literals;
    m.doc() = "C++ backend of beamformer capture";

    py::class_<session_config>(m, "SessionConfig", "Configuration data for the backend")
        .def(py::init<const std::string &>(), "filename"_a)
        .def_readwrite("filename", &session_config::filename)
        .def_readwrite("endpoints_str", &session_config::endpoints_str)
        .def_property("interface_address", &session_config::get_interface_address, &session_config::set_interface_address)
        .def_readwrite("max_packet", &session_config::max_packet)
        .def_readwrite("buffer_size", &session_config::buffer_size)
        .def_readwrite("live_heaps_per_substream", &session_config::live_heaps_per_substream)
        .def_readwrite("ring_slots", &session_config::ring_slots)
        .def_readwrite("ibv", &session_config::ibv)
        .def_readwrite("comp_vector", &session_config::comp_vector)
        .def_readwrite("network_affinity", &session_config::network_affinity)
        .def_readwrite("disk_affinity", &session_config::disk_affinity)
        .def_readwrite("direct", &session_config::direct)
        .def_readwrite("channels", &session_config::channels)
        .def_readwrite("stats_int_time", &session_config::stats_int_time)
        .def_readwrite("heaps_per_slice_time", &session_config::heaps_per_slice_time)
        .def_readwrite("ticks_between_spectra", &session_config::ticks_between_spectra)
        .def_readwrite("spectra_per_heap", &session_config::spectra_per_heap)
        .def_readwrite("channels_per_heap", &session_config::channels_per_heap)
        .def_readwrite("sync_time", &session_config::sync_time)
        .def_readwrite("bandwidth", &session_config::bandwidth)
        .def_readwrite("center_freq", &session_config::center_freq)
        .def_readwrite("scale_factor_timestamp", &session_config::scale_factor_timestamp)
        .def_readwrite("channel_offset", &session_config::channel_offset)
        .def("add_endpoint", &session_config::add_endpoint, "bind_host"_a, "port"_a)
        .def_property("stats_interface_address", &session_config::get_stats_interface_address, &session_config::set_stats_interface_address)
        .def("set_stats_endpoint", &session_config::set_stats_endpoint, "host"_a, "port"_a)
        .def("validate", &session_config::validate)
    ;
    py::class_<receiver_counters>(m, "ReceiverCounters", "Heap counters for a live capture session")
        .def(py::init<>())
        .def_readwrite("heaps", &receiver_counters::heaps)
        .def_readwrite("bytes", &receiver_counters::bytes)
        .def_readwrite("packets", &receiver_counters::packets)
        .def_readwrite("batches", &receiver_counters::batches)
        .def_readwrite("max_batch", &receiver_counters::max_batch)
        .def_readwrite("total_heaps", &receiver_counters::total_heaps)
        .def_readwrite("too_old_heaps", &receiver_counters::too_old_heaps)
        .def_readwrite("incomplete_heaps", &receiver_counters::incomplete_heaps)
        .def_readwrite("metadata_heaps", &receiver_counters::metadata_heaps)
        .def_readwrite("bad_timestamp_heaps", &receiver_counters::bad_timestamp_heaps)
        .def_readwrite("bad_channel_heaps", &receiver_counters::bad_channel_heaps)
        .def_readwrite("bad_length_heaps", &receiver_counters::bad_length_heaps)
        .def_readwrite("raw_heaps", &receiver_counters::raw_heaps)
    ;
    py::class_<session>(m, "Session", "Capture session")
        .def(py::init<const session_config &>(), "config"_a)
        .def("join", &session::join)
        .def("stop_stream", &session::stop_stream)
        .def_property_readonly("counters", &session::get_counters)
        .def_property_readonly("first_timestamp", &session::get_first_timestamp)
        .def("add_tcp_reader", &session::add_tcp_reader)
    ;

    py::object logging_module = py::module::import("logging");
    py::object my_logger_obj = logging_module.attr("getLogger")("katsdpbfingest.bf_ingest");
    set_logger(my_logger_obj);

    py::module atexit_mod = py::module::import("atexit");
    atexit_mod.attr("register")(py::cpp_function(clear_logger));

    spead2::register_logging();
    spead2::register_atexit();
}

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

#include <string>
#include <pybind11/pybind11.h>
#include <spead2/common_logging.h>
#include <spead2/py_common.h>
#include "common.h"
#include "session.h"

namespace py = pybind11;

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
        .def_readwrite("channel_offset", &session_config::channel_offset)
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
    py::object my_logger = logging_module.attr("getLogger")("katsdpbfingest.bf_ingest_session");
    spead2::set_log_function(spead2::log_function_python(spead2_logger));
    set_logger(my_logger);

    return m.ptr();
}

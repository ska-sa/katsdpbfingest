#include <string>
#include <utility>
#include <pybind11/pybind11.h>
#include <boost/asio.hpp>
#include <spead2/py_common.h>
#include "common.h"

static std::unique_ptr<spead2::log_function_python> logger;

void log_message(spead2::log_level level, const std::string &msg)
{
    (*logger)(level, msg);
}

void set_logger(pybind11::object logger_object)
{
    logger.reset(new spead2::log_function_python(logger_object));
}

void clear_logger()
{
    logger.reset();
}

std::vector<int> affinity_vector(int affinity)
{
    if (affinity < 0)
        return {};
    else
        return {affinity};
}

std::size_t slice::bytes(q::samples n)
{
    return 2 * sizeof(std::int8_t) * n.get();
}

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

void session_config::set_stats_endpoint(const std::string &host, std::uint16_t port)
{
    stats_endpoint = boost::asio::ip::udp::endpoint(
        boost::asio::ip::address_v4::from_string(host), port);
}

std::string session_config::get_stats_interface_address() const
{
    return stats_interface_address.to_string();
}

void session_config::set_stats_interface_address(const std::string &address)
{
    stats_interface_address = boost::asio::ip::address_v4::from_string(address);
}

units::freq_system session_config::get_freq_system() const
{
    return units::freq_system(channels_per_heap, channels / channels_per_heap);
}

units::time_system session_config::get_time_system() const
{
    return units::time_system(ticks_between_spectra,
                              spectra_per_heap,
                              heaps_per_slice_time);
}

const session_config &session_config::validate() const
{
    if (channels <= 0)
        throw std::invalid_argument("channels <= 0");
    if (channels_per_heap <= 0)
        throw std::invalid_argument("channels_per_heap <= 0");
    if (spectra_per_heap <= 0)
        throw std::invalid_argument("spectra_per_heap <= 0");
    if (spectra_per_heap >= 32768)
        throw std::invalid_argument("spectra_per_heap >= 32768");
    if (ticks_between_spectra <= 0)
        throw std::invalid_argument("ticks_between_spectra <= 0");
    if (sync_time <= 0)
        throw std::invalid_argument("sync_time <= 0");
    if (bandwidth <= 0)
        throw std::invalid_argument("bandwidth <= 0");
    if (center_freq <= 0)
        throw std::invalid_argument("center_freq <= 0");
    if (scale_factor_timestamp <= 0)
        throw std::invalid_argument("scale_factor_timestamp <= 0");
    if (heaps_per_slice_time <= 0)
        throw std::invalid_argument("heaps_per_slice_time <= 0");
    return *this;
}

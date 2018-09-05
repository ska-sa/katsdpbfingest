#include <string>
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

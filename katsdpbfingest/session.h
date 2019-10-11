#ifndef SESSION_H
#define SESSION_H

#include <cstdint>
#include <cstddef>
#include <future>
#include "common.h"
#include "receiver.h"
#include "session.h"

class session
{
private:
    const session_config config;
    receiver recv;
    std::future<void> run_future;

    void run_impl();  // internal implementation of run
    void run();       // runs in a separate thread

public:
    explicit session(const session_config &config);
    ~session();

    void join();
    void stop_stream();

    receiver_counters get_counters() const;
    std::int64_t get_first_timestamp() const;

    // For unit tests
    void add_tcp_reader(const spead2::socket_wrapper<boost::asio::ip::tcp::acceptor> &acceptor);
};

#endif // SESSION_H

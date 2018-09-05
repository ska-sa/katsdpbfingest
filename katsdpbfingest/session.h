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

#endif // SESSION_H

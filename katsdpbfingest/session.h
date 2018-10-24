#ifndef SESSION_H
#define SESSION_H

#include <cstdint>
#include <cstddef>
#include <future>
#include <mutex>
#include "common.h"
#include "receiver.h"
#include "session.h"

struct session_counters
{
    std::int64_t heaps = 0;    ///< Heaps actually received
    std::int64_t bytes = 0;    ///< Bytes of payload actually received
    std::int64_t total_heaps = 0;   ///< Heaps we expected to receive (based on timestamps)
};

class session
{
private:
    const session_config config;
    receiver recv;
    std::future<void> run_future;

    mutable std::mutex counters_mutex;
    session_counters counters;

    void run_impl();  // internal implementation of run
    void run();       // runs in a separate thread

public:
    explicit session(const session_config &config);
    ~session();

    void join();
    void stop_stream();

    session_counters get_counters() const;
    std::int64_t get_first_timestamp() const;
};

#endif // SESSION_H

#ifndef STATS_H
#define STATS_H

#include <cstdint>
#include <complex>
#include <string>
#include <vector>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include <boost/noncopyable.hpp>
#include "common.h"

/**
 * Statistics collection for signal displays.
 *
 * Because the values sent are tiny, transmission of each heap is done as fast
 * as possible and synchronously, rather than asynchronously at some defined
 * rate. This avoids the need to dedicate yet another thread to data
 * transmission, and the heaps should be small enough to fit entirely into
 * the various buffers between source and sink.
 *
 * When stats collection and disk write are both enabled, they run on the same
 * thread, so there is benefit in running much faster than real-time to make
 * more time available for disk writes.
 */
class stats_collector
{
private:
    /**
     * Backing data store for the dynamic data in a single heap. This is
     * grouped into its own structure to allow for double-buffering in
     * future. It's non-copyable because the heap is pre-constructed with
     * raw pointers.
     */
    struct transmit_data : public boost::noncopyable
    {
        spead2::send::heap heap;
        /** Ratio power_spectrum / weight. The
         * imaginary part is all zeros.
         */
        std::vector<std::complex<float>> power_spectrum;
        std::vector<float> saturated;     ///< Fraction of data saturated
        std::vector<std::uint8_t> flags;  ///< just data_lost if all samples lost
        std::uint64_t timestamp;  ///< centre, in centiseconds since Unix epoch

        transmit_data(const session_config &config);
    };

    /// Accumulated power per channel
    std::vector<std::uint64_t> power_spectrum;
    /// Accumulated number of saturated samples per channel
    std::vector<float> saturated;
    /// Number of valid samples collected
    std::vector<std::uint64_t> weight;
    /// Persist allocation of data to send (only used transiently)
    transmit_data data;

    // Constants copied from the session_config
    int spectra_per_heap;
    double sync_time;
    double scale_factor_timestamp;

    std::int64_t interval;   ///< transmit interval, in timestamp units
    std::int64_t start_timestamp = -1; //</ first timestamp of current accumulation

    boost::asio::io_service io_service;
    spead2::send::udp_stream stream;
    spead2::send::heap data_heap;

    /// Synchronously send a heap
    void send_heap(const spead2::send::heap &heap);

    /// Flush the currently accumulated statistics
    void transmit();

public:
    stats_collector(const session_config &config);
    ~stats_collector();

    /// Add a new slice of data
    void add(const slice &s);
};

#endif // STATS_H

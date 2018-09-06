#ifndef STATS_H
#define STATS_H

#include <cstdint>
#include <complex>
#include <string>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include <boost/noncopyable.hpp>
#include <vector>
#include "common.h"

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
        /* Ratio power_spectrum / power_spectrum_weight. The
         * imaginary part is all zeros.
         */
        std::vector<std::complex<float>> power_spectrum;
        std::vector<std::uint8_t> flags;  // just data_lost if all samples lost
        std::uint64_t timestamp;  // centre, in centiseconds since Unix epoch

        transmit_data(const session_config &config);
    };

    // Accumulated power per channel
    std::vector<std::uint64_t> power_spectrum;
    // Number of samples in matching entry of power_spectrum
    std::vector<std::uint64_t> power_spectrum_weight;
    // Persist allocation of data to send (only used transiently)
    transmit_data data;

    int spectra_per_heap;
    double sync_time;
    double scale_factor_timestamp;

    std::int64_t interval;   // transmit interval, in timestamp units
    std::int64_t start_timestamp = -1; // first timestamp of current accumulation

    boost::asio::io_service io_service;
    spead2::send::udp_stream stream;
    spead2::send::heap data_heap;

    void send_heap(const spead2::send::heap &heap);
    void transmit();

public:
    stats_collector(const session_config &config);
    ~stats_collector();

    void add(const slice &s);
};

#endif // STATS_H

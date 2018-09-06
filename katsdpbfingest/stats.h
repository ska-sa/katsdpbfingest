#ifndef STATS_H
#define STATS_H

#include <cstdint>
#include <complex>
#include <string>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include <vector>
#include "common.h"

class stats_collector
{
private:
    // Accumulated power per channel
    std::vector<std::uint64_t> power_spectrum;
    // Number of samples in matching entry of power_spectrum
    std::vector<std::uint64_t> power_spectrum_weight;
    /* Ratio of power_spectrum and power_spectrum_weight. It is only
     * updated immediately prior to transmission.
     */
    std::vector<std::complex<float>> power_spectrum_send;

    int spectra_per_heap;
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

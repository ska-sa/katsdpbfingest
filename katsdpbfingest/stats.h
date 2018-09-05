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
    spead2::send::heap data_heap;

    int spectra_per_heap;
    std::int64_t interval;   // transmit interval, in timestamp units
    std::int64_t last_sent_timestamp = -1;

    boost::asio::io_service io_service;
    spead2::send::udp_stream stream;

    void send_heap(const spead2::send::heap &heap);
    void transmit(std::int64_t timestamp);

public:
    stats_collector(const boost::asio::ip::udp::endpoint &endpoint,
                    const boost::asio::ip::address &interface_address,
                    int channels, int spectra_per_heap);
    ~stats_collector();

    void add(const slice &s);
};

#endif // STATS_H

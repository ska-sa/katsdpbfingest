#include <vector>
#include <complex>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include "common.h"
#include "stats.h"

// Taken from https://docs.google.com/spreadsheets/d/1XojAI9O9pSSXN8vyb2T97Sd875YCWqie8NY8L02gA_I/edit#gid=0
static constexpr int id_n_bls = 0x1008;
static constexpr int id_n_chans = 0x1009;
static constexpr int id_center_freq = 0x1011;
static constexpr int id_bandwidth = 0x1013;
static constexpr int id_sd_timestamp = 0x3502;
static constexpr int id_sd_data = 0x3507;
static constexpr int id_sd_data_index = 0x3509;


void stats_collector::send_heap(const spead2::send::heap &heap)
{
    auto handler = [](const boost::system::error_code &ec,
                      spead2::item_pointer_t bytes_transferred)
    {
        if (ec)
            log_format(spead2::log_level::warning, "Error sending heap: %s", ec.message());
    };
    stream.async_send_heap(heap, handler);
    io_service.run();
    io_service.reset();
}

stats_collector::stats_collector(const boost::asio::ip::udp::endpoint &endpoint,
                                 const boost::asio::ip::address &interface_address,
                                 int channels, int spectra_per_heap)
    : power_spectrum(channels), power_spectrum_weight(channels), power_spectrum_send(channels),
    spectra_per_heap(spectra_per_heap),
    stream(io_service, endpoint,
           spead2::send::stream_config(8872),
           spead2::send::udp_stream::default_buffer_size,
           1, interface_address)
{
    assert(spectra_per_heap < 32768); // otherwise overflows can occur
    spead2::send::heap start_heap;
    start_heap.add_start();
    send_heap(start_heap);
    // TODO: send descriptors
    interval = 1;  // TODO: base on scale_factor_timestamp

    // TODO: all the other fields
    data_heap.add_item(id_sd_data,
                       power_spectrum_send.data(),
                       power_spectrum_send.size() * sizeof(power_spectrum_send[0]), false);
}

void stats_collector::add(const slice &s)
{
    int channels = power_spectrum.size();
    int heaps = s.present.size();
    int channels_per_heap = channels / heaps;
    const int8_t *data = reinterpret_cast<const int8_t *>(s.data.get());
    for (int heap = 0; heap < heaps; heap++)
    {
        if (!s.present[heap])
            continue;
        int start_channel = heap * channels_per_heap;
        // TODO: split out into function compiled for multiple instruction sets
        for (int channel = start_channel; channel < start_channel + channels_per_heap; channel++)
        {
            const int8_t *cdata = data + channel * spectra_per_heap * 2;
            uint32_t accum = 0;
            for (int i = 0; i < spectra_per_heap * 2; i++)
            {
                int16_t v = cdata[i];
                accum += v * v;
            }
            power_spectrum[channel] += accum;
            power_spectrum_weight[channel] += spectra_per_heap;
        }
    }

    if (s.timestamp - last_sent_timestamp >= interval)
        transmit(s.timestamp);
}

void stats_collector::transmit(std::int64_t timestamp)
{
    int channels = power_spectrum.size();
    for (int i = 0; i < channels; i++)
        power_spectrum_send[i] = float(power_spectrum[i]) / power_spectrum_weight[i];

    send_heap(data_heap);
    last_sent_timestamp = timestamp;

    std::fill(power_spectrum.begin(), power_spectrum.end(), 0);
    std::fill(power_spectrum_weight.begin(), power_spectrum_weight.end(), 0);
}

stats_collector::~stats_collector()
{
    spead2::send::heap heap;
    heap.add_end();
    send_heap(heap);
}

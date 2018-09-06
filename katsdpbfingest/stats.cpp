#include <vector>
#include <complex>
#include <cassert>
#include <utility>
#include <sstream>
#include <algorithm>
#include <type_traits>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include <spead2/common_endian.h>
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

static spead2::flavour make_flavour()
{
    return spead2::flavour(4, 64, 48);
}

static void add_descriptor(spead2::send::heap &heap,
                           spead2::s_item_pointer_t id,
                           std::string name, std::string description,
                           const std::vector<int> &shape,
                           std::string dtype)
{
    spead2::descriptor d;
    d.id = id;
    d.name = std::move(name);
    d.description = std::move(description);
    std::ostringstream numpy_header;
    numpy_header << "{'shape': (";
    for (auto s : shape)
    {
        assert(s >= 0);
        numpy_header << s << ", ";
    }
    char endian_char = spead2::htobe(std::uint16_t(0x1234)) == 0x1234 ? '>' : '<';
    numpy_header << "), 'fortran_order': False, 'descr': '" << endian_char << dtype << "'}";
    d.numpy_header = numpy_header.str();
    heap.add_descriptor(d);
}

template<typename T,
         typename SFINAE = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
static void add_constant(spead2::send::heap &heap, spead2::s_item_pointer_t id, const T &value)
{
    std::unique_ptr<std::uint8_t[]> dup(new std::uint8_t[sizeof(T)]);
    std::memcpy(dup.get(), &value, sizeof(T));
    heap.add_item(id, dup.get(), sizeof(T), true);
    heap.add_pointer(std::move(dup));
}

stats_collector::transmit_data::transmit_data(const session_config &config)
    : power_spectrum(config.channels), heap(make_flavour())
{
    add_descriptor(heap, id_sd_data, "sd_data", "Power spectrum",
                   {config.channels, 1, 2}, "f4");
    heap.add_item(id_sd_data,
                  power_spectrum.data(),
                  power_spectrum.size() * sizeof(power_spectrum[0]), false);
    add_descriptor(heap, id_sd_timestamp, "sd_timestamp", "Timestamp of this sd frame in centiseconds since epoch",
                   {}, "u8");
    heap.add_item(id_sd_timestamp, &timestamp, sizeof(timestamp), true);

    // TODO: more fields
    add_descriptor(heap, id_n_chans, "n_chans", "Number of channels", {}, "u4");
    add_constant(heap, id_n_chans, std::uint32_t(config.channels));
    add_descriptor(heap, id_bandwidth, "bandwidth", "The analogue bandwidth of the digitally processed signal, in Hz.",
                   {}, "f4");
    add_constant(heap, id_bandwidth, config.bandwidth);
    add_descriptor(heap, id_center_freq, "center_freq", "The center frequency of the DBE in Hz, 64-bit IEEE floating-point number.",
                   {}, "f4");
    add_constant(heap, id_center_freq, config.center_freq);
}

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

stats_collector::stats_collector(const session_config &config)
    : power_spectrum(config.channels),
    power_spectrum_weight(config.channels),
    data(config),
    spectra_per_heap(config.spectra_per_heap),
    sync_time(config.sync_time),
    scale_factor_timestamp(config.scale_factor_timestamp),
    stream(io_service, config.stats_endpoint,
           spead2::send::stream_config(8872),
           spead2::send::udp_stream::default_buffer_size,
           1, config.stats_interface_address)
{
    assert(spectra_per_heap < 32768); // otherwise overflows can occur
    spead2::send::heap start_heap;
    start_heap.add_start();
    send_heap(start_heap);

    auto interval_align = std::int64_t(config.spectra_per_heap) * config.ticks_between_spectra;
    interval = std::int64_t(std::round(config.stats_int_time * config.scale_factor_timestamp));
    interval = interval / interval_align * interval_align;
    if (interval <= 0)
        interval = interval_align;
}

void stats_collector::add(const slice &s)
{
    if (start_timestamp == -1)
        start_timestamp = s.timestamp;
    assert(s.timestamp >= start_timestamp); // timestamps must be provided in order
    if (s.timestamp >= start_timestamp + interval)
    {
        transmit();
        // Get start timestamp that is of form first_timestamp + i * interval
        start_timestamp += (s.timestamp - start_timestamp) / interval * interval;
    }

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
}

void stats_collector::transmit()
{
    int channels = power_spectrum.size();
    for (int i = 0; i < channels; i++)
        data.power_spectrum[i] = float(power_spectrum[i]) / power_spectrum_weight[i];
    double timestamp_unix = sync_time + (start_timestamp + 0.5 * interval) / scale_factor_timestamp;
    // Convert to centiseconds, since that's what signal display uses
    data.timestamp = std::uint64_t(std::round(timestamp_unix * 100.0));

    send_heap(data.heap);

    std::fill(power_spectrum.begin(), power_spectrum.end(), 0);
    std::fill(power_spectrum_weight.begin(), power_spectrum_weight.end(), 0);
}

stats_collector::~stats_collector()
{
    if (start_timestamp != -1)
        transmit();
    spead2::send::heap heap;
    heap.add_end();
    send_heap(heap);
}
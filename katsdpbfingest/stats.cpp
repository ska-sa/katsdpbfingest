#include <vector>
#include <complex>
#include <cassert>
#include <utility>
#include <sstream>
#include <algorithm>
#include <functional>
#include <type_traits>
#include <iterator>
#include <spead2/send_stream.h>
#include <spead2/send_udp.h>
#include <spead2/common_endian.h>
#include "common.h"
#include "stats.h"

#define TARGET "default"
#include "stats_ops.h"
#undef TARGET

#define TARGET "avx"
#include "stats_ops.h"
#undef TARGET

#define TARGET "avx2"
#include "stats_ops.h"
#undef TARGET

// Taken from https://docs.google.com/spreadsheets/d/1XojAI9O9pSSXN8vyb2T97Sd875YCWqie8NY8L02gA_I/edit#gid=0
static constexpr int id_n_bls = 0x1008;
static constexpr int id_n_chans = 0x1009;
static constexpr int id_center_freq = 0x1011;
static constexpr int id_bandwidth = 0x1013;
static constexpr int id_bls_ordering = 0x100C;
static constexpr int id_sd_data = 0x3501;
static constexpr int id_sd_timestamp = 0x3502;
static constexpr int id_sd_flags = 0x3503;
static constexpr int id_sd_timeseries = 0x3504;
static constexpr int id_sd_percspectrum = 0x3505;
static constexpr int id_sd_percspectrumflags = 0x3506;
static constexpr int id_sd_blmxdata = 0x3507;
static constexpr int id_sd_blmxflags = 0x3508;
static constexpr int id_sd_data_index = 0x3509;
static constexpr int id_sd_blmx_n_chans = 0x350A;
static constexpr int id_sd_flag_fraction = 0x350B;
static constexpr int id_sd_timeseriesabs = 0x3510;

/// Make SPEAD 64-48 flavour
static spead2::flavour make_flavour()
{
    return spead2::flavour(4, 64, 48);
}

/**
 * Helper to add a descriptor to a heap with numpy-style descriptor.
 *
 * The @a dtype should not have an endianness indicator. It will be added by
 * this function.
 */
static void add_descriptor(spead2::send::heap &heap,
                           spead2::s_item_pointer_t id,
                           const std::string &name, const std::string &description,
                           const std::vector<int> &shape,
                           const std::string &dtype)
{
    spead2::descriptor d;
    d.id = id;
    d.name = name;
    d.description = description;
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

/**
 * Add a value to a heap, copying the data.
 *
 * The heap takes ownership of the copied data, so it is not necessary for @a
 * value to remain live after the call.
 */
template<typename T,
         typename SFINAE = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
static void add_constant(spead2::send::heap &heap, spead2::s_item_pointer_t id, const T &value)
{
    std::unique_ptr<std::uint8_t[]> dup(new std::uint8_t[sizeof(T)]);
    std::memcpy(dup.get(), &value, sizeof(T));
    heap.add_item(id, dup.get(), sizeof(T), true);
    heap.add_pointer(std::move(dup)); // Give the heap ownership of the memory
}

/**
 * Add a value to a heap, copying the data.
 *
 * This overload takes the value as a string.
 */
static void add_constant(spead2::send::heap &heap, spead2::s_item_pointer_t id,
                         const std::string &value)
{
    /* This code is written to handle arbitrary containers, but the function
     * isn't templated for them because the required SFINAE checks to ensure
     * that it is safe become very messy.
     */
    auto first = std::begin(value);
    auto last = std::end(value);
    auto n = last - first;
    using T = std::iterator_traits<decltype(first)>::value_type;
    std::unique_ptr<std::uint8_t[]> dup(new std::uint8_t[sizeof(T) * n]);
    std::uninitialized_copy(first, last, reinterpret_cast<T *>(dup.get()));
    heap.add_item(id, dup.get(), sizeof(T) * n, false);
    heap.add_pointer(std::move(dup));
}

/**
 * Helper to call @ref add_descriptor and add a variable vector item.
 */
template<typename T,
         typename SFINAE = typename std::enable_if<std::is_trivially_copyable<T>::value>::type>
static void add_vector(spead2::send::heap &heap, spead2::s_item_pointer_t id,
                       const std::string &name, const std::string &description,
                       const std::vector<int> &shape,
                       const std::string &dtype,
                       std::vector<T> &data)
{
    add_descriptor(heap, id, name, description, shape, dtype);
#ifndef NDEBUG
    std::size_t expected_size = std::accumulate(shape.begin(), shape.end(), std::size_t(1),
                                                std::multiplies<>());
    assert(expected_size == data.size());
#endif
    heap.add_item(id, data.data(), data.size() * sizeof(T), false);
}

/**
 * Helper to call @ref add_descriptor and @ref add_constant with zeros.
 *
 * This is used just to fake up items that are currently expected by
 * timeplot. It should be removed once timeplot supports beamformer signal
 * displays.
 */
template<typename T>
static void add_zeros(spead2::send::heap &heap, spead2::s_item_pointer_t id,
                      const std::string &name,
                      const std::vector<int> &shape,
                      const std::string &dtype)
{
    std::size_t n = 1;
    for (int s : shape)
        n *= s;
    add_descriptor(heap, id, name, "Dummy item", shape, dtype);
    add_constant(heap, id, std::string(n * sizeof(T), '\0'));
}

stats_collector::transmit_data::transmit_data(const session_config &config)
    : heap(make_flavour()),
    data(2 * config.channels),
    flags(2 * config.channels)
{
    using namespace std::literals;

    add_vector(heap, id_sd_data, "sd_data",
               "Power spectrum and fraction of samples that are saturated. These are encoded "
               "as baselines with inputs m999h,m999h and m999v,m999v respectively.",
               {config.channels, 2, 2}, "f4", data);
    add_vector(heap, id_sd_flags, "sd_flags", "8bit packed flags for each data point.",
               {config.channels, 2}, "u1", flags);
    add_descriptor(heap, id_sd_timestamp, "sd_timestamp", "Timestamp of this sd frame in centiseconds since epoch",
                   {}, "u8");
    heap.add_item(id_sd_timestamp, &timestamp, sizeof(timestamp), true);

    // TODO: more fields
    add_descriptor(heap, id_n_chans, "n_chans", "Number of channels", {}, "u4");
    add_constant(heap, id_n_chans, std::uint32_t(config.channels));
    add_descriptor(heap, id_bandwidth, "bandwidth", "The analogue bandwidth of the digitally processed signal, in Hz.",
                   {}, "f8");
    add_constant(heap, id_bandwidth, config.bandwidth);
    add_descriptor(heap, id_center_freq, "center_freq", "The center frequency of the DBE in Hz, 64-bit IEEE floating-point number.",
                   {}, "f8");
    add_constant(heap, id_center_freq, config.center_freq);
    add_descriptor(heap, id_bls_ordering, "bls_ordering", "Baseline output ordering.",
                   {2, 2}, "S5");  // Must match the chosen input name
    // TODO: use a proper name. The m999h/v is to fit the signal display's expectations
    add_constant(heap, id_bls_ordering, "m999hm999hm999vm999v"s);
    add_descriptor(heap, id_sd_data_index, "sd_data_index", "Indices for transmitted sd_data.",
                   {2}, "u4");
    add_constant(heap, id_sd_data_index, std::array<std::uint32_t, 2>{{0, 1}});

    // TODO: fields below here are just for testing against a correlator signal
    // display server, and should mostly be removed.
    add_descriptor(heap, id_sd_blmx_n_chans, "sd_blmx_n_chans", "Dummy item", {}, "u4");
    add_constant(heap, id_sd_blmx_n_chans, std::uint32_t(config.channels));
    add_vector(heap, id_sd_blmxdata, "sd_blmxdata", "Dummy item",
               {config.channels, 2, 2}, "f4", data);
    add_vector(heap, id_sd_blmxflags, "sd_blmxflags", "Dummy item",
               {config.channels, 2}, "u1", flags);
    add_zeros<float>(heap, id_sd_flag_fraction, "sd_flag_fraction", {2, 8}, "f4");
    add_zeros<float>(heap, id_sd_timeseries, "sd_timeseries", {2, 2}, "f4");
    add_zeros<float>(heap, id_sd_timeseriesabs, "sd_timeseriesabs", {2}, "f4");
    add_zeros<float>(heap, id_sd_percspectrum, "sd_percspectrum", {config.channels, 40}, "f4");
    add_zeros<std::uint8_t>(heap, id_sd_percspectrumflags, "sd_percspectrumflags",
                            {config.channels, 40}, "u1");
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
    // io_service will refuse to run again unless reset is called
    io_service.reset();
}

stats_collector::stats_collector(const session_config &config)
    : power_spectrum(config.channels),
    saturated(config.channels),
    weight(config.channels),
    data(config),
    spectra_per_heap(config.spectra_per_heap),
    sync_time(config.sync_time),
    scale_factor_timestamp(config.scale_factor_timestamp),
    stream(io_service, config.stats_endpoint,
           spead2::send::stream_config(8872),
           spead2::send::udp_stream::default_buffer_size,
           1, config.stats_interface_address)
{
    /* spectra_per_heap is checked by session_config::validate, so this
     * is just a sanity check. It's necessary to limit spectra_per_heap
     * to avoid overflowing narrow integers during accumulation. If there
     * is a future need for larger values it can be handled by splitting
     * the accumulations into shorter pieces.
     */
    assert(spectra_per_heap < 32768);
    spead2::send::heap start_heap;
    start_heap.add_start();
    send_heap(start_heap);

    // Convert config.stats_int_time to timestamp units and round to whole heaps
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

    // Update the statistics using the heaps in the slice
    int channels = power_spectrum.size();
    int heaps = s.present.size();
    int channels_per_heap = channels / heaps;
    const int8_t *data = reinterpret_cast<const int8_t *>(s.data.get());
    for (int heap = 0; heap < heaps; heap++)
    {
        if (!s.present[heap])
            continue;
        int start_channel = heap * channels_per_heap;
        for (int channel = start_channel; channel < start_channel + channels_per_heap; channel++)
        {
            const int8_t *cdata = data + channel * spectra_per_heap * 2;
            power_spectrum[channel] += power_sum(spectra_per_heap, cdata);
            saturated[channel] += count_saturated(spectra_per_heap, cdata);
            weight[channel] += spectra_per_heap;
        }
    }
}

void stats_collector::transmit()
{
    // Compute derived values
    int channels = power_spectrum.size();
    std::fill(data.flags.begin(), data.flags.end(), 0);
    for (int i = 0; i < channels; i++)
    {
        if (weight[i] != 0)
        {
            float w = 1.0f / weight[i];
            data.data[2 * i] = power_spectrum[i] * w;
            data.data[2 * i + 1] = saturated[i] * w;
        }
        else
        {
            data.data[2 * i] = 0;
            data.data[2 * i + 1] = 0;
            data.flags[2 * i] = data_lost;
            data.flags[2 * i + 1] = data_lost;
        }
    }
    double timestamp_unix = sync_time + (start_timestamp + 0.5 * interval) / scale_factor_timestamp;
    // Convert to centiseconds, since that's what signal display uses
    data.timestamp = std::uint64_t(std::round(timestamp_unix * 100.0));

    send_heap(data.heap);

    // Reset for the next interval
    std::fill(power_spectrum.begin(), power_spectrum.end(), 0);
    std::fill(saturated.begin(), saturated.end(), 0);
    std::fill(weight.begin(), weight.end(), 0);
}

stats_collector::~stats_collector()
{
    // If start_timestamp != -1 then we received at least one heap, and from
    // then on we will always have an interval in progress.
    if (start_timestamp != -1)
        transmit();
    // Send stop heap
    spead2::send::heap heap;
    heap.add_end();
    send_heap(heap);
}

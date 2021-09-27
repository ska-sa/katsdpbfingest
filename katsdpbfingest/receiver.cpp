#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <functional>
#include <chrono>
#include <mutex>
#include <algorithm>
#include <numeric>
#include <spead2/recv_stream.h>
#include <spead2/recv_chunk_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/recv_udp_ibv.h>
#include <spead2/recv_udp.h>
#include <spead2/recv_tcp.h>
#include <spead2/recv_mem.h>
#include <spead2/recv_utils.h>
#include <spead2/common_features.h>
#include <spead2/common_ringbuffer.h>
#include <spead2/common_endian.h>
#include <pybind11/pybind11.h>
#include "common.h"
#include "receiver.h"

// TODO: only used for gil_scoped_release. Would be nice to find a way to avoid
// having this file depend on pybind11.
namespace py = pybind11;

constexpr std::size_t receiver::window_size;
constexpr int receiver::bf_raw_id;
constexpr int receiver::timestamp_id;
constexpr int receiver::frequency_id;

std::unique_ptr<slice> receiver::make_slice()
{
    std::unique_ptr<slice> s{new slice};
    auto slice_samples =
        time_sys.convert_one<units::slices::time, units::spectra>()
        * freq_sys.convert_one<units::slices::freq, units::channels>();
    auto present_size =
        time_sys.convert_one<units::slices::time, units::heaps::time>()
        * freq_sys.convert_one<units::slices::freq, units::heaps::freq>();
    s->data = make_aligned<std::uint8_t>(slice::bytes(slice_samples));
    // Fill the data just to pre-fault it
    std::memset(s->data.get(), 0, slice::bytes(slice_samples));
    s->present = std::make_unique<std::uint8_t[]>(present_size.get());
    s->present_size = present_size.get();
    return s;
}

void receiver::emplace_readers()
{
#if SPEAD2_USE_IBV
    if (use_ibv)
    {
        log_format(spead2::log_level::info, "Listening on %1% with interface %2% using ibverbs",
                   config.endpoints_str, config.interface_address);
        stream.emplace_reader<spead2::recv::udp_ibv_reader>(
            spead2::recv::udp_ibv_config()
                .set_endpoints(config.endpoints)
                .set_interface_address(config.interface_address)
                .set_max_size(config.max_packet)
                .set_buffer_size(config.buffer_size)
                .set_comp_vector(config.comp_vector));
    }
    else
#endif
    if (!config.interface_address.is_unspecified())
    {
        log_format(spead2::log_level::info, "Listening on %1% with interface %2%",
                   config.endpoints_str, config.interface_address);
        for (const auto &endpoint : config.endpoints)
            stream.emplace_reader<spead2::recv::udp_reader>(
                endpoint, config.max_packet, config.buffer_size,
                config.interface_address);
    }
    else
    {
        log_format(spead2::log_level::info, "Listening on %1%", config.endpoints_str);
        for (const auto &endpoint : config.endpoints)
            stream.emplace_reader<spead2::recv::udp_reader>(
                endpoint, config.max_packet, config.buffer_size);
    }
}

void receiver::add_tcp_reader(const spead2::socket_wrapper<boost::asio::ip::tcp::acceptor> &acceptor)
{
    stream.emplace_reader<spead2::recv::tcp_reader>(
        acceptor.copy(stream.get_io_service()), config.max_packet);
}

bool receiver::parse_timestamp_channel(
    q::ticks timestamp, q::channels channel,
    q::spectra &spectrum,
    std::size_t &heap_offset, q::heaps &present_idx,
    std::uint64_t *counter_stats, bool quiet)
{
    if (timestamp < first_timestamp)
    {
        if (!quiet)
        {
            counter_stats[counters::before_start_heaps]++;
            log_format(spead2::log_level::debug, "timestamp %1% pre-dates start %2%, discarding",
                       timestamp, first_timestamp);
        }
        return false;
    }
    bool have_first = (first_timestamp != q::ticks(-1));
    q::ticks rel = !have_first ? q::ticks(0) : timestamp - first_timestamp;
    q::ticks one_heap_ts = time_sys.convert_one<units::heaps::time, units::ticks>();
    q::channels one_slice_f = freq_sys.convert_one<units::slices::freq, units::channels>();
    q::channels one_heap_f = freq_sys.convert_one<units::heaps::freq, units::channels>();
    if (rel % one_heap_ts)
    {
        if (!quiet)
        {
            counter_stats[counters::bad_timestamp_heaps]++;
            log_format(spead2::log_level::debug, "timestamp %1% is not properly aligned to %2%, discarding",
                       timestamp, one_heap_ts);
        }
        return false;
    }
    if (channel % one_heap_f)
    {
        if (!quiet)
        {
            counter_stats[counters::bad_channel_heaps]++;
            log_format(spead2::log_level::debug, "frequency %1% is not properly aligned to %2%, discarding",
                       channel, one_heap_f);
        }
        return false;
    }
    if (channel < channel_offset || channel >= one_slice_f + channel_offset)
    {
        if (!quiet)
        {
            counter_stats[counters::bad_channel_heaps]++;
            log_format(spead2::log_level::debug, "frequency %1% is outside of range [%2%, %3%), discarding",
                       channel, channel_offset, one_slice_f + channel_offset);
        }
        return false;
    }

    channel -= channel_offset;
    spectrum = time_sys.convert_down<units::spectra>(rel);

    // Pre-compute some conversion factors
    q::slices_t one_slice(1);
    q::heaps_t slice_heaps = time_sys.convert<units::heaps::time>(one_slice);
    q::spectra slice_spectra = time_sys.convert<units::spectra>(slice_heaps);

    // Compute slice-local coordinates
    q::heaps_t time_heaps = time_sys.convert_down<units::heaps::time>(spectrum % slice_spectra);
    q::samples time_samples = time_sys.convert<units::spectra>(time_heaps) * q::channels(1);
    q::heaps_f freq_heaps = freq_sys.convert_down<units::heaps::freq>(channel);
    heap_offset = slice::bytes(time_samples + channel * slice_spectra);
    present_idx = time_heaps * q::heaps_f(1) + freq_heaps * slice_heaps;

    if (!have_first)
        first_timestamp = timestamp;
    return true;
}

void receiver::finish_slice(slice &s, std::uint64_t *counter_stats) const
{
    std::size_t n_present = std::accumulate(s.present.get(),
                                            s.present.get() + s.present_size, std::size_t(0));
    s.n_present = q::heaps(n_present);
    if (n_present == 0)
        return;

    q::slices_t slice_id{s.chunk_id};
    s.spectrum = time_sys.convert<units::spectra>(slice_id);
    s.timestamp = time_sys.convert<units::ticks>(s.spectrum) + first_timestamp;

#if 0 // TODO move these to session?
    counters.heaps += s.n_present.get();
    counters.bytes += s.n_present.get() * payload_size;
    q::slices_t slice_id = time_sys.convert_down<units::slices::time>(s.spectrum);
    std::int64_t total_heaps = (slice_id.get() + 1) * s.present.size();
    counters.total_heaps = std::max(counters.total_heaps, total_heaps);
#endif

    // If any heaps got lost, fill them with zeros
    if (n_present != s.present_size)
    {
        const q::heaps_f slice_heaps_f = freq_sys.convert_one<units::slices::freq, units::heaps::freq>();
        const q::heaps_t slice_heaps_t = time_sys.convert_one<units::slices::time, units::heaps::time>();
        const std::size_t heap_row =
            slice::bytes(time_sys.convert_one<units::heaps::time, units::spectra>() * q::channels(1));
        const q::channels heap_channels = freq_sys.convert_one<units::heaps::freq, units::channels>();
        const q::spectra stride = time_sys.convert_one<units::slices::time, units::spectra>();
        const std::size_t stride_bytes = slice::bytes(stride * q::channels(1));
        q::heaps present_idx{0};
        for (q::heaps_f i{0}; i < slice_heaps_f; i++)
            for (q::heaps_t j{0}; j < slice_heaps_t; j++, present_idx++)
                if (!s.present[present_idx.get()])
                {
                    auto start_channel = freq_sys.convert<units::channels>(i);
                    const q::samples dst_offset =
                        start_channel * stride
                        + time_sys.convert<units::spectra>(j) * q::channels(1);
                    std::uint8_t *ptr = s.data.get() + slice::bytes(dst_offset);
                    for (q::channels k{0}; k < heap_channels; k++, ptr += stride_bytes)
                        std::memset(ptr, 0, heap_row);
                }
    }
}

void receiver::packet_memcpy(const spead2::memory_allocator::pointer &allocation,
                             const spead2::recv::packet_header &packet)
{
    typedef unit_system<std::int64_t, units::bytes, units::channels> stride_system;
    stride_system src_sys(
        slice::bytes(time_sys.convert_one<units::heaps::time, units::spectra>() * q::channels(1)));
    stride_system dst_sys(
        slice::bytes(time_sys.convert_one<units::slices::time, units::spectra>() * q::channels(1)));
    q::bytes src_stride = src_sys.convert_one<units::channels, units::bytes>();
    /* Copy one channel at a time. Some extra index manipulation is needed
     * because the packet might have partial channels at the start and end,
     * or only a middle part of a channel.
     *
     * Some of this could be optimised by handling the complete channels
     * separately from the leftovers (particularly since in MeerKAT we expect
     * there not to be any leftovers).
     *
     * coordinates are all relative to the start of the heap.
     */
    q::bytes payload_start(packet.payload_offset);
    q::bytes payload_length(packet.payload_length);
    q::bytes payload_end = payload_start + payload_length;
    q::channels channel_start = src_sys.convert_down<units::channels>(payload_start);
    q::channels channel_end = src_sys.convert_up<units::channels>(payload_end);
    for (q::channels c = channel_start; c < channel_end; c++)
    {
        q::bytes src_start = src_sys.convert<units::bytes>(c);
        q::bytes src_end = src_start + src_stride;
        q::bytes dst_start = dst_sys.convert<units::bytes>(c);
        if (payload_start > src_start)
        {
            dst_start += payload_start - src_start;
            src_start = payload_start;
        }
        if (payload_end < src_end)
            src_end = payload_end;
        std::memcpy(allocation.get() + dst_start.get(),
                    packet.payload + (src_start - payload_start).get(),
                    (src_end - src_start).get());
    }
}

void receiver::place(spead2::recv::chunk_place_data *data, std::size_t data_size)
{
    // This assert will fail if the spead2 library at runtime is older than the
    // version we were compiled against.
    assert(data_size >= sizeof(*data));

    q::ticks timestamp{data->items[0]};
    q::channels channel{data->items[1]};
    spead2::item_pointer_t heap_size = data->items[2];
    // Metadata heaps will be missing timestamp and channel
    std::uint64_t *counter_stats = data->batch_stats + counter_base;
    if (timestamp == q::ticks(-1) || channel == q::channels(-1))
    {
        counter_stats[counters::metadata_heaps]++;
        return;
    }

    q::spectra spectrum;
    std::size_t heap_offset;
    q::heaps present_idx;
    if (!parse_timestamp_channel(timestamp, channel, spectrum, heap_offset, present_idx,
                                 counter_stats))
        return;

    if (heap_size != payload_size)
    {
        counter_stats[counters::bad_length_heaps]++;
        log_format(spead2::log_level::debug, "heap has wrong length (%1% != %2%), discarding",
                   heap_size, payload_size);
        return;
    }

    data->chunk_id = time_sys.convert_down<units::slices::time>(spectrum).get();
    data->heap_offset = heap_offset;
    data->heap_index = present_idx.get();
}

void receiver::graceful_stop()
{
    // Abuse the fact that a mem_reader calls stop_received when it gets to the end
    stream.emplace_reader<spead2::recv::mem_reader>(nullptr, 0);
}

void receiver::stop()
{
    stream.stop();
}

spead2::recv::stream_config receiver::make_stream_config()
{
    spead2::recv::stream_config stream_config;
    stream_config.set_max_heaps(
        std::max(1, config.channels / config.channels_per_heap) * config.live_heaps_per_substream);
    stream_config.set_memcpy(
        [this](const spead2::memory_allocator::pointer &allocation,
               const spead2::recv::packet_header &packet)
        {
            packet_memcpy(allocation, packet);
        });
    stream_config.add_stat("katsdpbfingest.metadata_heaps");
    stream_config.add_stat("katsdpbfingest.bad_timestamp_heaps");
    stream_config.add_stat("katsdpbfingest.bad_channel_heaps");
    stream_config.add_stat("katsdpbfingest.bad_length_heaps");
    stream_config.add_stat("katsdpbfingest.before_start_heaps");
    return stream_config;
}

spead2::recv::chunk_stream_config receiver::make_chunk_stream_config()
{
    spead2::recv::chunk_stream_config config;
    config.set_items({timestamp_id, frequency_id, spead2::item_id::HEAP_LENGTH_ID});
    config.set_max_chunks(window_size);
    config.set_place([this](spead2::recv::chunk_place_data *data, std::size_t data_size)
    {
        place(data, data_size);
    });
    config.set_ready([this](std::unique_ptr<spead2::recv::chunk> &&chunk, std::uint64_t *batch_stats)
    {
        finish_slice(static_cast<slice &>(*chunk), batch_stats + counter_base);
    });
    return config;
}

using ringbuffer_t = spead2::ringbuffer<std::unique_ptr<spead2::recv::chunk>>;

receiver::receiver(const session_config &config)
    : config(config),
    channel_offset(config.channel_offset),
    freq_sys(config.get_freq_system()),
    time_sys(config.get_time_system()),
    payload_size(2 * sizeof(std::int8_t) * config.spectra_per_heap * config.channels_per_heap),
    worker(1, affinity_vector(config.network_affinity)),
    stream(
        worker,
        make_stream_config(),
        make_chunk_stream_config(),
        std::make_shared<ringbuffer_t>(config.ring_slots),
        std::make_shared<ringbuffer_t>(window_size + config.ring_slots + 1))
{
    py::gil_scoped_release gil;

    counter_base = stream.get_config().get_stat_index("metadata_heaps");
    try
    {
        if (config.ibv)
        {
            use_ibv = true;
#if !SPEAD2_USE_IBV
            log_message(spead2::log_level::warning, "Not using ibverbs because support is not compiled in");
            use_ibv = false;
#endif
            if (use_ibv)
            {
                for (const auto &endpoint : config.endpoints)
                    if (!endpoint.address().is_multicast())
                    {
                        log_format(spead2::log_level::warning, "Not using ibverbs because endpoint %1% is not multicast",
                               endpoint);
                        use_ibv = false;
                        break;
                    }
            }
            if (use_ibv && config.interface_address.is_unspecified())
            {
                log_message(spead2::log_level::warning, "Not using ibverbs because interface address is not specified");
                use_ibv = false;
            }
        }

        for (std::size_t i = 0; i < window_size + config.ring_slots + 1; i++)
            stream.add_free_chunk(make_slice());

        emplace_readers();
    }
    catch (std::exception &)
    {
        /* Normally we can rely on the destructor to call stop() (which is
         * necessary to ensure that the stream isn't going to make more calls
         * into the receiver while it is being destroyed), but an exception
         * thrown from the constructor does not cause the destructor to get
         * called.
         *
         * TODO: is this still necessary? The stream now interacts directly
         * with the ringbuffers.
         */
        stop();
        throw;
    }
}

receiver::~receiver()
{
    stop();
}

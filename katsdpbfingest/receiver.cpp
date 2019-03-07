#include <cstddef>
#include <cstdint>
#include <memory>
#include <functional>
#include <chrono>
#include <mutex>
#include <algorithm>
#include <spead2/recv_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/recv_udp_ibv.h>
#include <spead2/recv_udp.h>
#include <spead2/recv_inproc.h>
#include <spead2/recv_utils.h>
#include <spead2/common_ringbuffer.h>
#include <spead2/common_endian.h>
#include <pybind11/pybind11.h>
#include "common.h"
#include "receiver.h"

// TODO: only used for gil_scoped_release. Would be nice to find a way to avoid
// having this file depend on pybind11.
namespace py = pybind11;

bf_stream::~bf_stream()
{
    stop();
}

bf_stream::bf_stream(receiver &recv, std::size_t max_heaps)
    : spead2::recv::stream(recv.worker, 0, max_heaps),
    recv(recv)
{
}

void bf_stream::heap_ready(spead2::recv::live_heap &&heap)
{
    if (!heap.is_contiguous())
        return;
    recv.heap_ready(spead2::recv::heap(std::move(heap)));
}

void bf_stream::stop_received()
{
    spead2::recv::stream::stop_received();
    recv.stop_received();
}


bf_raw_allocator::bf_raw_allocator(receiver &recv) : recv(recv)
{
}

bf_raw_allocator::pointer bf_raw_allocator::allocate(std::size_t size, void *hint)
{
    std::uint8_t *ptr = nullptr;
    if (hint)
        ptr = recv.allocate(size, *reinterpret_cast<const spead2::recv::packet_header *>(hint));
    if (ptr)
        return pointer(ptr, deleter(shared_from_this(), (void *) std::uintptr_t(true)));
    else
        return spead2::memory_allocator::allocate(size, hint);
}

void bf_raw_allocator::free(std::uint8_t *ptr, void *user)
{
    if (!user)
        delete[] ptr;
}

constexpr std::size_t receiver::window_size;
constexpr int receiver::bf_raw_id;
constexpr int receiver::timestamp_id;
constexpr int receiver::frequency_id;

slice receiver::make_slice()
{
    slice s;
    auto slice_samples =
        time_sys.convert_one<units::slices::time, units::spectra>()
        * freq_sys.convert_one<units::slices::freq, units::channels>();
    auto present_size =
        time_sys.convert_one<units::slices::time, units::heaps::time>()
        * freq_sys.convert_one<units::slices::freq, units::heaps::freq>();
    s.data = make_aligned<std::uint8_t>(slice::bytes(slice_samples));
    // Fill the data just to pre-fault it
    std::memset(s.data.get(), 0, slice::bytes(slice_samples));
    s.present.resize(present_size.get());
    return s;
}

void receiver::emplace_readers()
{
    if (!config.inproc_queues.empty())
    {
        log_format(spead2::log_level::info, "Listening to %1% in-process queues",
                   config.inproc_queues.size());
        for (const auto &queue : config.inproc_queues)
            stream.emplace_reader<spead2::recv::inproc_reader>(queue);
    }
#if SPEAD2_USE_IBV
    else if (use_ibv)
    {
        log_format(spead2::log_level::info, "Listening on %1% with interface %2% using ibverbs",
                   config.endpoints_str, config.interface_address);
        stream.emplace_reader<spead2::recv::udp_ibv_reader>(
            config.endpoints, config.interface_address,
            config.max_packet,
            config.buffer_size,
            config.comp_vector);
    }
#endif
    else if (!config.interface_address.is_unspecified())
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

bool receiver::parse_timestamp_channel(
    q::ticks timestamp, q::channels channel,
    q::spectra &spectrum,
    std::size_t &heap_offset, q::heaps &present_idx)
{
    if (timestamp < first_timestamp)
    {
        log_format(spead2::log_level::warning, "timestamp %1% pre-dates start %2%, discarding",
                   timestamp, first_timestamp);
        return false;
    }
    bool have_first = (first_timestamp != q::ticks(-1));
    q::ticks rel = !have_first ? q::ticks(0) : timestamp - first_timestamp;
    q::ticks one_heap_ts = time_sys.convert_one<units::heaps::time, units::ticks>();
    q::channels one_slice_f = freq_sys.convert_one<units::slices::freq, units::channels>();
    q::channels one_heap_f = freq_sys.convert_one<units::heaps::freq, units::channels>();
    if (rel % one_heap_ts)
    {
        log_format(spead2::log_level::warning, "timestamp %1% is not properly aligned to %2%, discarding",
                   timestamp, one_heap_ts);
        return false;
    }
    if (channel % one_heap_f)
    {
        log_format(spead2::log_level::warning, "frequency %1% is not properly aligned to %2%, discarding",
                   channel, one_heap_f);
        return false;
    }
    if (channel < channel_offset || channel >= one_slice_f + channel_offset)
    {
        log_format(spead2::log_level::warning, "frequency %1% is outside of range [%2%, %3%), discarding",
                   channel, channel_offset, one_slice_f + channel_offset);
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

slice *receiver::get_slice(q::ticks timestamp, q::spectra spectrum)
{
    try
    {
        q::slices_t slice_id = time_sys.convert_down<units::slices::time>(spectrum);
        slice *s = get(slice_id.get());
        if (!s)
        {
            return nullptr;
        }
        if (!s->data)
        {
            *s = free_ring.pop();
            s->timestamp = timestamp;
            s->spectrum = time_sys.convert<units::spectra>(slice_id);
            s->n_present = q::heaps(0);
            // clear all the bits by resizing down to zero then back to original size
            auto orig_size = s->present.size();
            s->present.clear();
            s->present.resize(orig_size);
        }
        return s;
    }
    catch (spead2::ringbuffer_stopped)
    {
        return nullptr;
    }
}

std::uint8_t *receiver::allocate(std::size_t size, const spead2::recv::packet_header &packet)
{
    if (state != state_t::DATA || size != payload_size)
        return nullptr;
    spead2::recv::pointer_decoder decoder(packet.heap_address_bits);
    // Try to extract the timestamp and frequency
    q::ticks timestamp{-1};
    q::channels channel{0};
    for (int i = 0; i < packet.n_items; i++)
    {
        spead2::item_pointer_t pointer = spead2::load_be<spead2::item_pointer_t>(packet.pointers + i * sizeof(pointer));
        if (decoder.is_immediate(pointer))
        {
            int id = decoder.get_id(pointer);
            if (id == timestamp_id)
                timestamp = q::ticks(decoder.get_immediate(pointer));
            else if (id == frequency_id)
                channel = q::channels(decoder.get_immediate(pointer));
        }
    }
    if (timestamp != q::ticks(-1))
    {
        // It's a data heap, so we should be able to use it
        q::spectra spectrum;
        std::size_t heap_offset;
        q::heaps present_idx;
        if (parse_timestamp_channel(timestamp, channel,
                                    spectrum, heap_offset, present_idx))
        {
            slice *s = get_slice(timestamp, spectrum);
            if (s)
                return s->data.get() + heap_offset;
        }
    }
    return nullptr;
}

void receiver::flush(slice &s)
{
    if (s.data)
    {
        counters.heaps += s.n_present.get();
        counters.bytes += s.n_present.get() * payload_size;
        q::slices_t slice_id = time_sys.convert_down<units::slices::time>(s.spectrum);
        std::int64_t total_heaps = (slice_id.get() + 1) * s.present.size();
        counters.total_heaps = std::max(counters.total_heaps, total_heaps);

        // If any heaps got lost, fill them with zeros
        if (s.n_present != q::heaps(s.present.size()))
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
        ring.push(std::move(s));
    }
    s.spectrum = q::spectra(-1);
}

void receiver::packet_memcpy(const spead2::memory_allocator::pointer &allocation,
                             const spead2::recv::packet_header &packet)
{
    if (!allocation.get_deleter().get_user())
        return;

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

void receiver::heap_ready(const spead2::recv::heap &h)
{
    if (state != state_t::DATA)
        return;
    q::ticks timestamp{-1};
    q::channels channel{0};
    const spead2::recv::item *data_item = nullptr;
    for (const auto &item : h.get_items())
    {
        if (item.id == timestamp_id)
            timestamp = q::ticks(item.immediate_value);
        else if (item.id == frequency_id)
            channel = q::channels(item.immediate_value);
        else if (item.id == bf_raw_id)
            data_item = &item;
    }
    // Metadata heaps won't have a timestamp
    if (timestamp == q::ticks(-1) || data_item == nullptr)
        return;

    q::spectra spectrum;
    std::size_t heap_offset;
    q::heaps present_idx;
    if (!parse_timestamp_channel(timestamp, channel, spectrum, heap_offset, present_idx))
    {
        counters.bad_metadata_heaps++;
        return;
    }
    if (data_item->length != payload_size)
    {
        counters.bad_metadata_heaps++;
        log_format(spead2::log_level::warning, "bf_raw item has wrong length (%1% != %2%), discarding",
                   data_item->length, payload_size);
        return;
    }

    slice *s = get_slice(timestamp, spectrum);
    if (!s)
    {
        // Chunk has been flushed already, or we have been stopped
        if (state == state_t::DATA)
            counters.too_old_heaps++;
        return;
    }

    std::uint8_t *ptr = s->data.get() + heap_offset;
    if (data_item->ptr != ptr)
    {
        log_message(spead2::log_level::warning, "heap was not reconstructed in-place");
        throw std::runtime_error("heap was not reconstructed in-place");
    }
    if (!s->present[present_idx.get()])
    {
        s->n_present++;
        s->present[present_idx.get()] = true;
    }
}

void receiver::refresh_counters_periodic(const boost::system::error_code &ec)
{
    using namespace std::placeholders;
    if (ec == boost::asio::error::operation_aborted)
        return;
    else if (ec)
        log_message(spead2::log_level::warning, "refresh_counters timer error");
    counters_timer.expires_from_now(std::chrono::milliseconds(10));
    counters_timer.async_wait(std::bind(&receiver::refresh_counters_periodic, this, _1));
    refresh_counters();
}

void receiver::refresh_counters()
{
    auto stream_stats = stream.get_stats();
    std::lock_guard<std::mutex> lock(counters_mutex);
    counters_public = counters;
    counters_public.packets = stream_stats.packets;
    counters_public.batches = stream_stats.batches;
    counters_public.raw_heaps = stream_stats.heaps;
    counters_public.max_batch = stream_stats.max_batch;
    counters_public.incomplete_heaps = stream_stats.incomplete_heaps_evicted;
}

receiver_counters receiver::get_counters() const
{
    std::lock_guard<std::mutex> lock(counters_mutex);
    return counters_public;
}

void receiver::stop_received()
{
    if (state == state_t::DATA)
    {
        try
        {
            flush_all();
        }
        catch (spead2::ringbuffer_stopped)
        {
            // can get here if we were called via receiver::stop
        }
    }
    ring.stop();
    counters_timer.cancel();
    refresh_counters();
    state = state_t::STOP;
}

void receiver::graceful_stop()
{
    stream.get_strand().post([this] { stop_received(); });
}

void receiver::stop()
{
    /* Stop the ring first, so that we unblock the internals if they
     * are waiting for space in ring.
     */
    ring.stop();
    stream.stop();
}

receiver::receiver(const session_config &config)
    : window<slice, receiver>(window_size),
    config(config),
    channel_offset(config.channel_offset),
    freq_sys(config.get_freq_system()),
    time_sys(config.get_time_system()),
    payload_size(2 * sizeof(std::int8_t) * config.spectra_per_heap * config.channels_per_heap),
    worker(1, affinity_vector(config.network_affinity)),
    stream(*this, std::max(1, config.channels / config.channels_per_heap) * config.live_heaps_per_substream),
    counters_timer(worker.get_io_service()),
    ring(config.ring_slots),
    free_ring(window_size + config.ring_slots + 1)
{
    py::gil_scoped_release gil;

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
            free_ring.push(make_slice());

        std::shared_ptr<spead2::memory_allocator> allocator =
            std::make_shared<bf_raw_allocator>(*this);
        stream.set_memory_allocator(std::move(allocator));
        stream.set_memcpy(
            [this](const spead2::memory_allocator::pointer &allocation,
                   const spead2::recv::packet_header &packet)
            {
                packet_memcpy(allocation, packet);
            });
        stream.set_allow_unsized_heaps(false);

        emplace_readers();
        // Start periodic updates
        refresh_counters_periodic(boost::system::error_code());
    }
    catch (std::exception)
    {
        /* Normally we can rely on the destructor to call stop() (which is
         * necessary to ensure that the stream isn't going to make more calls
         * into the receiver while it is being destroyed), but an exception
         * thrown from the constructor does not cause the destructor to get
         * called.
         */
        stop();
        throw;
    }
}

receiver::~receiver()
{
    stop();
}

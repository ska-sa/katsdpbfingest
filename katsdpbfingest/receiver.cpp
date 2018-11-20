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
    std::size_t slice_size = 2 * spectra_per_heap * channels;
    std::size_t present_size = channels / channels_per_heap;
    s.data = make_aligned<std::uint8_t>(slice_size);
    // Fill the data just to pre-fault it
    std::memset(s.data.get(), 0, slice_size);
    s.present.resize(present_size);
    return s;
}

void receiver::emplace_readers()
{
    std::ostringstream endpoints_str;
    bool first = true;
    for (const auto &endpoint : config.endpoints)
    {
        if (!first)
            endpoints_str << ',';
        first = false;
        endpoints_str << endpoint;
    }
#if SPEAD2_USE_IBV
    if (use_ibv)
    {
        log_format(spead2::log_level::info, "Listening on %1% with interface %2% using ibverbs",
                   endpoints_str.str(), config.interface_address);
        stream.emplace_reader<spead2::recv::udp_ibv_reader>(
            config.endpoints, config.interface_address,
            spead2::recv::udp_ibv_reader::default_max_size,
            config.buffer_size,
            config.comp_vector);
    }
    else
#endif
    {
        if (!config.interface_address.is_unspecified())
        {
            log_format(spead2::log_level::info, "Listening on %1% with interface %2%",
                       endpoints_str.str(), config.interface_address);
            for (const auto &endpoint : config.endpoints)
                stream.emplace_reader<spead2::recv::udp_reader>(
                    endpoint, spead2::recv::udp_reader::default_max_size, config.buffer_size,
                    config.interface_address);
        }
        else
        {
            log_format(spead2::log_level::info, "Listening on %1%", endpoints_str.str());
            for (const auto &endpoint : config.endpoints)
                stream.emplace_reader<spead2::recv::udp_reader>(
                    endpoint, spead2::recv::udp_reader::default_max_size, config.buffer_size);
        }
    }
}

bool receiver::parse_timestamp_channel(
    std::int64_t timestamp, int channel,
    std::int64_t &spectrum,
    std::size_t &heap_offset, std::size_t &present_idx)
{
    if (timestamp < first_timestamp)
    {
        log_format(spead2::log_level::warning, "timestamp %1% pre-dates start %2%, discarding",
                   timestamp, first_timestamp);
        return false;
    }
    std::int64_t rel = (first_timestamp == -1) ? 0 : timestamp - first_timestamp;
    if (rel % (ticks_between_spectra * spectra_per_heap) != 0)
    {
        log_format(spead2::log_level::warning, "timestamp %1% is not properly aligned to %2%, discarding",
                   timestamp, ticks_between_spectra * spectra_per_heap);
        return false;
    }
    if (channel % channels_per_heap != 0)
    {
        log_format(spead2::log_level::warning, "frequency %1% is not properly aligned to %2%, discarding",
                   channel, channels_per_heap);
        return false;
    }
    if (channel < channel_offset || channel >= channels + channel_offset)
    {
        log_format(spead2::log_level::warning, "frequency %1% is outside of range [%2%, %3%), discarding",
                   channel, channel_offset, channels + channel_offset);
        return false;
    }

    channel -= channel_offset;
    spectrum = rel / ticks_between_spectra;
    heap_offset = 2 * channel * spectra_per_heap;
    present_idx = channel / channels_per_heap;

    if (first_timestamp == -1)
        first_timestamp = timestamp;
    return true;
}

slice *receiver::get_slice(std::int64_t timestamp, std::int64_t spectrum)
{
    try
    {
        slice *s = get(spectrum / spectra_per_heap);
        if (!s)
        {
            return nullptr;
        }
        if (!s->data)
        {
            *s = free_ring.pop();
            s->timestamp = timestamp;
            s->spectrum = spectrum;
            s->n_present = 0;
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
    std::int64_t timestamp = -1;
    int channel = 0;
    for (int i = 0; i < packet.n_items; i++)
    {
        spead2::item_pointer_t pointer = spead2::load_be<spead2::item_pointer_t>(packet.pointers + i * sizeof(pointer));
        if (decoder.is_immediate(pointer))
        {
            int id = decoder.get_id(pointer);
            if (id == timestamp_id)
                timestamp = decoder.get_immediate(pointer);
            else if (id == frequency_id)
                channel = decoder.get_immediate(pointer);
        }
    }
    if (timestamp != -1)
    {
        // It's a data heap, so we should be able to use it
        std::int64_t spectrum;
        std::size_t heap_offset, present_idx;
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
        counters.heaps += s.n_present;
        counters.bytes += s.n_present * payload_size;
        std::int64_t total_heaps = (s.spectrum / spectra_per_heap + 1) * s.present.size();
        counters.total_heaps = std::max(counters.total_heaps, total_heaps);

        // If any heaps got lost, fill them with zeros
        if (s.n_present != s.present.size())
        {
            std::size_t offset = 0;
            for (std::size_t i = 0; i < s.present.size(); i++, offset += payload_size)
                if (!s.present[i])
                    std::memset(s.data.get() + offset, 0, payload_size);
        }
        ring.push(std::move(s));
    }
    s.spectrum = -1;
}

void receiver::heap_ready(const spead2::recv::heap &h)
{
    if (state != state_t::DATA)
        return;
    std::int64_t timestamp = -1;
    int channel = 0;
    const spead2::recv::item *data_item = nullptr;
    for (const auto &item : h.get_items())
    {
        if (item.id == timestamp_id)
            timestamp = item.immediate_value;
        else if (item.id == frequency_id)
            channel = item.immediate_value;
        else if (item.id == bf_raw_id)
            data_item = &item;
    }
    // Metadata heaps won't have a timestamp, and metadata will continue to
    // arrive after we've seen the initial metadata heap.
    if (timestamp == -1 || data_item == nullptr)
        return;

    std::int64_t spectrum;
    std::size_t heap_offset, present_idx;
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
        std::memcpy(ptr, data_item->ptr, payload_size);
    }
    if (!s->present[present_idx])
    {
        s->n_present++;
        s->present[present_idx] = true;
    }
}

void receiver::refresh_counters(const boost::system::error_code &ec)
{
    using namespace std::placeholders;
    if (ec == boost::asio::error::operation_aborted)
        return;
    else if (ec)
        log_message(spead2::log_level::warning, "refresh_counters timer error");
    counters_timer.expires_from_now(std::chrono::milliseconds(1));
    counters_timer.async_wait(std::bind(&receiver::refresh_counters, this, _1));

    auto stream_stats = stream.get_stats();
    std::lock_guard<std::mutex> lock(counters_mutex);
    counters_public = counters;
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
    state = state_t::STOP;
}

void receiver::graceful_stop()
{
    stream.get_strand().post([this] { stop_received(); });
}

void receiver::stop()
{
    counters_timer.cancel();
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
    channels(config.channels),
    ticks_between_spectra(config.ticks_between_spectra),
    spectra_per_heap(config.spectra_per_heap),
    channels_per_heap(config.channels_per_heap),
    payload_size(2 * spectra_per_heap * channels_per_heap),
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

        stream.set_memcpy(spead2::MEMCPY_NONTEMPORAL);
        std::shared_ptr<spead2::memory_allocator> allocator =
            std::make_shared<bf_raw_allocator>(*this);
        stream.set_memory_allocator(std::move(allocator));

        emplace_readers();
        refresh_counters(boost::system::error_code());
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

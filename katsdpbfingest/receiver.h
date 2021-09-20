#ifndef RECEIVER_H
#define RECEIVER_H

#include <cstdint>
#include <memory>
#include <spead2/recv_stream.h>
#include <spead2/recv_chunk_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/common_ringbuffer.h>
#include <spead2/common_memory_allocator.h>
#include <spead2/py_common.h>
#include "common.h"

class receiver;

namespace counters
{

// These must be kept in sync with the calls to add_stat in make_stream_config
static constexpr std::size_t metadata_heaps = 0;
static constexpr std::size_t bad_timestamp_heaps = 1;
static constexpr std::size_t bad_channel_heaps = 2;
static constexpr std::size_t bad_length_heaps = 3;
static constexpr std::size_t before_start_heaps = 4;

} // namespace counters

/**
 * Collects data from the network. It has a built-in thread pool with one
 * thread, and runs almost entirely on that thread.
 */
class receiver
{
private:
    const session_config config;
    bool use_ibv = false;

    /// Depth of window
    static constexpr std::size_t window_size = 8;

    // Metadata copied from or computed from the session_config
    const q::channels channel_offset;
    const units::freq_system freq_sys;
    const units::time_system time_sys;
    const std::size_t payload_size;

    // Hard-coded item IDs
    static constexpr int bf_raw_id = 0x5000;
    static constexpr int timestamp_id = 0x1600;
    static constexpr int frequency_id = 0x4103;

    q::ticks first_timestamp{-1};

    spead2::thread_pool worker;
    /// Index of the first custom statistic
    std::size_t counter_base;

    /// Create a single fully-allocated slice
    std::unique_ptr<slice> make_slice();

    /// Create the stream configuration for the stream
    spead2::recv::stream_config make_stream_config();

    /// Create the chunk configuration for the stream
    spead2::recv::chunk_stream_config make_chunk_stream_config();

    /// Add the readers to the already-allocated stream
    void emplace_readers();

    /**
     * Process a timestamp and channel number from a heap into more useful
     * indices. Note: this function modifies state by setting @ref
     * first_timestamp if this is the first (valid) call. If it is invalid,
     * a suitable error counter is incremented.
     *
     * @param timestamp        ADC timestamp
     * @param channel          Channel number of first channel in heap
     * @param[out] spectrum    Index of first spectrum in heap, counting from 0
     *                         for first heap
     * @param[out] heap_offset Byte offset from start of slice data for this heap
     * @param[out] present_idx Position in @ref slice::present to record this heap
     * @param batch_stats      Pointer to stream's statistics for updating
     * @param quiet            If true, do not log or increment counters on bad heaps
     *
     * @retval true  if @a timestamp and @a channel are valid
     * @retval false otherwise, and a message is logged
     */
    bool parse_timestamp_channel(
        q::ticks timestamp, q::channels channel,
        q::spectra &spectrum,
        std::size_t &heap_offset, q::heaps &present_idx,
        std::uint64_t *batch_stats,
        bool quiet = false);

    /**
     * Copy contents of one packet to a slice.
     */
    void packet_memcpy(const spead2::memory_allocator::pointer &allocated,
                       const spead2::recv::packet_header &packet);

    /**
     * Finish processing on a slice before flushing it.
     *
     * This fills in any missing heaps with zeros, and populates the
     * slice-specific fields.
     *
     * If the slice is completely empty, however, it only populates n_present.
     */
    void finish_slice(slice &s, std::uint64_t *counter_stats) const;

    /**
     * chunk_place_function for the underlying stream.
     */
    void place(spead2::recv::chunk_place_data *data, std::size_t size);

public:
    spead2::recv::chunk_ring_stream<> stream;

    /**
     * Retrieve first timestamp, or -1 if no data was received.
     * It is only valid to call this once the receiver has been stopped
     * or a non-empty slice has been received.
     */
    q::ticks get_first_timestamp() const
    {
        return first_timestamp;
    }

    explicit receiver(const session_config &config);
    ~receiver();

    /// Add a TCP socket receiver to a running receiver (for testing only!)
    void add_tcp_reader(const spead2::socket_wrapper<boost::asio::ip::tcp::acceptor> &acceptor);

    /// Stop immediately, without flushing any slices
    void stop();

    /// Asynchronously stop, allowing buffered slices to flush
    void graceful_stop();
};

#endif // RECEIVER_H

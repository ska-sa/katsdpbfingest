#ifndef RECEIVER_H
#define RECEIVER_H

#include <cstdint>
#include <spead2/recv_stream.h>
#include <spead2/recv_heap.h>
#include <spead2/common_ringbuffer.h>
#include <spead2/common_memory_allocator.h>
#include "common.h"

class receiver;

/**
 * Wrapper around base stream class that forwards to a receiver.
 */
class bf_stream : public spead2::recv::stream
{
private:
    receiver &recv;

    virtual void heap_ready(spead2::recv::live_heap &&heap) override;
    virtual void stop_received() override;

public:
    bf_stream(receiver &recv, std::size_t max_heaps);
    virtual ~bf_stream() override;
};

/**
 * Allocator that returns pointers inside slices.
 *
 * It retains a pointer to the stream, but does not use this pointer while
 * freeing. It is thus safe to free the stream even if there are still
 * allocated values outstanding.
 *
 * If it allocates memory from a slice, it sets a non-zero value in the
 * user field.
 */
class bf_raw_allocator : public spead2::memory_allocator
{
private:
    receiver &recv;

    virtual void free(std::uint8_t *ptr, void *user) override;

public:
    explicit bf_raw_allocator(receiver &recv);

    virtual pointer allocate(std::size_t size, void *hint) override;
};

/**
 * Collects data from the network, using custom stream classes. It has a
 * built-in thread pool with one thread, and runs almost entirely on that
 * thread.
 *
 * Class bf_stream is a thin stream wrapper that calls back into this class to
 * handle the received heaps.
 */
class receiver : private window<slice, receiver>
{
private:
    friend class bf_stream;
    friend class bf_raw_allocator;
    friend class window<slice, receiver>;

    enum class state_t
    {
        DATA,         ///< Receiving data
        STOP          ///< Have seen stop packet or been asked to stop
    };

    const session_config config;
    bool use_ibv = false;

    /// Depth of window
    static constexpr std::size_t window_size = 64;

    // Metadata copied from or computed from the session_config
    const int channel_offset;
    const int channels;
    const std::int64_t ticks_between_spectra;
    const int spectra_per_heap;
    const int channels_per_heap;
    const std::size_t payload_size;

    // Hard-coded item IDs
    static constexpr int bf_raw_id = 0x5000;
    static constexpr int timestamp_id = 0x1600;
    static constexpr int frequency_id = 0x4103;

    state_t state = state_t::DATA;
    std::int64_t first_timestamp = -1;

    spead2::thread_pool worker;
    bf_stream stream;

    /// Create a single fully-allocated slice
    slice make_slice();

    /// Add the readers to the already-allocated stream
    void emplace_readers();

    /**
     * Process a timestamp and channel number from a heap into more useful
     * indices. Note: this function modifies state by setting @ref
     * first_timestamp if this is the first (valid) call.
     *
     * @param timestamp        ADC timestamp
     * @param channel          Channel number of first channel in heap
     * @param[out] spectrum    Index of first spectrum in heap, counting from 0
     *                         for first heap
     * @param[out] heap_offset Byte offset from start of slice data for this heap
     * @param[out] present_idx Position in @ref slice::present to record this heap
     *
     * @retval true  if @a timestamp and @a channel are valid
     * @retval false otherwise, and a message is logged
     */
    bool parse_timestamp_channel(
        std::int64_t timestamp, int channel,
        std::int64_t &spectrum, std::size_t &heap_offset, std::size_t &present_idx);

    /**
     * Obtain a pointer to an allocated slice. It returns @c nullptr if the
     * timestamp is too far in the past.
     *
     * This can block if @c free_ring is empty.
     */
    slice *get_slice(std::int64_t timestamp, std::int64_t spectrum);

    /**
     * Find space within a slice. This is the backing implementation for
     * @ref bf_raw_allocator.
     *
     * If necessary, this pushes to the ring and pulls from the free ring, so
     * it can block.
     *
     * @return  A pointer to existing memory, or @c nullptr if this is not a
     *          valid data heap.
     */
    std::uint8_t *allocate(std::size_t size, const spead2::recv::packet_header &packet);

    /// Flush a single slice to the ringbuffer, if it has data
    void flush(slice &s);

    /// Called by bf_stream::heap_ready
    void heap_ready(const spead2::recv::heap &heap);
    /// Called by bf_stream::stop_received
    void stop_received();

public:
    /**
     * Filled (or partially filled) slices. These are guaranteed to be provided
     * to the consumer in order.
     */
    spead2::ringbuffer<slice> ring;

    /**
     * The consumer puts processed rings back here. It is used as a source of
     * pre-allocated objects.
     */
    spead2::ringbuffer<slice> free_ring;

    /**
     * Retrieve first timestamp, or -1 if no data was received.
     * It is only valid to call this once the receiver has been stopped.
     */
    std::int64_t get_first_timestamp() const
    {
        assert(state == state_t::STOP);
        return first_timestamp;
    }

    explicit receiver(const session_config &config);
    ~receiver();

    /// Stop immediately, without flushing any slices
    void stop();

    /// Asynchronously stop, allowing buffered slices to flush
    void graceful_stop();
};

#endif // RECEIVER_H

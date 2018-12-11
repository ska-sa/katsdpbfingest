#ifndef COMMON_H
#define COMMON_H

#include <memory>
#include <vector>
#include <string>
#include <cstdint>
#include <experimental/optional>
#include <boost/asio.hpp>
#include <boost/format.hpp>
#include <spead2/common_logging.h>
#include <spead2/recv_udp_ibv.h>

// Forward-declare to avoid sucking in pybind11.h
namespace pybind11 { class object; }

static constexpr std::uint8_t data_lost = 1 << 3;

void log_message(spead2::log_level level, const std::string &msg);
void set_logger(pybind11::object logger);
void clear_logger();

/**
 * Recursively push a variadic list of arguments into a @c boost::format. This
 * is copied from the spead2 codebase.
 */
static inline void apply_format(boost::format &formatter)
{
}

template<typename T0, typename... Ts>
static void apply_format(boost::format &formatter, T0 &&arg0, Ts&&... args)
{
    formatter % std::forward<T0>(arg0);
    apply_format(formatter, std::forward<Ts>(args)...);
}

template<typename... Ts>
static void log_format(spead2::log_level level, const std::string &format, Ts&&... args)
{
    boost::format formatter(format);
    apply_format(formatter, args...);
    log_message(level, formatter.str());
}

template<typename T>
struct free_delete
{
    void operator()(T *ptr) const
    {
        free(ptr);
    }
};

template<typename T>
using aligned_ptr = std::unique_ptr<T[], free_delete<T>>;

static constexpr int ALIGNMENT = 4096;   // For O_DIRECT file access

/**
 * Allocate memory that is aligned to a multiple of @c ALIGNMENT. This is used
 * with O_DIRECT.
 */
template<typename T>
static aligned_ptr<T> make_aligned(std::size_t elements)
{
    void *ptr = aligned_alloc(ALIGNMENT, elements * sizeof(T));
    if (!ptr)
        throw std::bad_alloc();
    return std::unique_ptr<T[], free_delete<T>>(static_cast<T*>(ptr));
}

/**
 * Return a vector that can be passed to a @c spead2::thread_pool constructor
 * to bind to core @a affinity, or to no core if it is negative.
 */
std::vector<int> affinity_vector(int affinity);

/**
 * Storage for all the heaps that share a timestamp.
 */
struct slice
{
    std::int64_t timestamp = -1;       ///< Timestamp from the heap
    std::int64_t spectrum = -1;        ///< Number of spectra since start
    unsigned int n_present = 0;        ///< Number of 1 bits in @a present
    aligned_ptr<std::uint8_t> data;    ///< Payload: channel-major, time-minor
    std::vector<bool> present;         ///< Bitmask of present heaps
};

/**
 * Generic class (using Curiously Recursive Template Pattern) for managing an
 * (unbounded) sequence of items, each of which is built up from smaller
 * items, which do not necessarily arrive strictly in order. This class
 * manages a fixed-size window of slots. The derived class can ask for access
 * to a specific element of the sequence, and if necessary the window is
 * advanced, flushing trailing items. The window is never retracted; if the
 * caller wants to access an item that is too old, it is refused.
 *
 * The subclass must provide a @c flush method to process an item from the
 * trailing edge and then reset its state. Note that @c flush may be called on
 * items that have never been accessed with @ref get, so subclasses must
 * handle this.
 */
template<typename T, typename Derived>
class window
{
public:
    typedef T value_type;

private:
    /**
     * The window itself. Item @c i is stored in index
     * <code>i % window_size</code>, provided that
     * <code>start &lt;= i &lt; start + window_size</code>.
     */
    std::vector<T> slots;
    std::size_t oldest = 0;      ///< Index into @ref slots of oldest item
    std::int64_t start = 0;      ///< ID of oldest item in window

    void flush_oldest();

public:
    // The args are passed to the constructor for T to construct the slots
    template<typename... Args>
    explicit window(std::size_t window_size, Args&&... args);

    /// Returns pointer to the slot for @a id, or @c nullptr if the window has moved on
    T *get(std::int64_t id);

    /// Flush the next @a window_size items (even if they have never been requested)
    void flush_all();
};

template<typename T, typename Derived>
template<typename... Args>
window<T, Derived>::window(std::size_t window_size, Args&&... args)
{
    slots.reserve(window_size);
    for (std::size_t i = 0; i < window_size; i++)
        slots.emplace_back(std::forward<Args>(args)...);
}

template<typename T, typename Derived>
void window<T, Derived>::flush_oldest()
{
    T &item = slots[oldest];
    static_cast<Derived *>(this)->flush(item);
    oldest++;
    if (oldest == slots.size())
        oldest = 0;
    start++;
}

template<typename T, typename Derived>
T *window<T, Derived>::get(std::int64_t id)
{
    if (id < start)
        return nullptr;
    // TODO: fast-forward mechanism for big differences?
    while (id - start >= std::int64_t(slots.size()))
        flush_oldest();
    std::size_t pos = id % slots.size();
    return &slots[pos];
}

template<typename T, typename Derived>
void window<T, Derived>::flush_all()
{
    for (std::size_t i = 0; i < slots.size(); i++)
        flush_oldest();
}

struct session_config
{
    std::experimental::optional<std::string> filename;
    std::vector<boost::asio::ip::udp::endpoint> endpoints;
    std::string endpoints_str;   ///< Human-readable version of endpoints
    boost::asio::ip::address interface_address;

    std::size_t max_packet = spead2::recv::udp_ibv_reader::default_max_size;
    std::size_t buffer_size = 32 * 1024 * 1024;
    int live_heaps_per_substream = 2;
    int ring_slots = 128;
    bool ibv = false;
    int comp_vector = 0;
    int network_affinity = -1;

    int disk_affinity = -1;
    bool direct = false;

    // First channel
    int channel_offset = 0;
    // Number of channels, counting from channel_offset
    int channels = -1;
    // Time (in seconds) over which to accumulate stats
    double stats_int_time = 1.0;

    // Metadata derived from telescope state.
    std::int64_t ticks_between_spectra = -1;
    int spectra_per_heap = -1;
    int channels_per_heap = -1;
    double sync_time = -1.0;
    double bandwidth = -1.0;
    double center_freq = -1.0;
    double scale_factor_timestamp = -1.0;

    boost::asio::ip::udp::endpoint stats_endpoint;
    boost::asio::ip::address stats_interface_address;

    explicit session_config(const std::string &filename);
    void add_endpoint(const std::string &bind_host, std::uint16_t port);
    std::string get_interface_address() const;
    void set_interface_address(const std::string &address);

    void set_stats_endpoint(const std::string &host, std::uint16_t port);
    std::string get_stats_interface_address() const;
    void set_stats_interface_address(const std::string &address);

    // Check that all required items have been set and return self.
    // Throws invalid_value if not.
    const session_config &validate() const;
};

#endif // COMMON_H

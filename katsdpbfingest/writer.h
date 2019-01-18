#ifndef WRITER_H
#define WRITER_H

#include <cstddef>
#include <H5Cpp.h>
#include "common.h"

class hdf5_bf_raw_writer
{
private:
    const unit_system<std::int64_t, units::channels, units::slices::freq> freq_sys;
    const unit_system<std::int64_t, units::bytes, units::spectra, units::slices::time> time_sys;
    q::bytes chunk_bytes;
    H5::DataSet dataset;

public:
    hdf5_bf_raw_writer(H5::Group &parent, int channels,
                       int spectra_per_slice,
                       const char *name);

    void add(const slice &c);
};

class hdf5_timestamps_writer
{
private:
    static constexpr hsize_t chunk = 1048576;
    H5::DataSet dataset;
    std::unique_ptr<std::uint64_t[], free_delete<std::uint64_t>> buffer;
    hsize_t n_buffer = 0;
    hsize_t n_written = 0;

    void flush();
public:
    const unit_system<std::int64_t, units::ticks, units::spectra, units::heaps::time> timestamp_sys;

    hdf5_timestamps_writer(H5::Group &parent,
                           std::int64_t ticks_between_spectra, int spectra_per_heap,
                           const char *name);
    ~hdf5_timestamps_writer();
    // Add a heap's worth of timestamps
    void add(q::ticks timestamp);
};

/**
 * Memory storage for an HDF5 chunk of flags data. This covers the whole band
 * and also many heaps in time.
 */
struct flags_chunk
{
    q::spectra spectrum{-1};
    aligned_ptr<std::uint8_t> data;

    explicit flags_chunk(q::heaps size);
};

class hdf5_flags_writer : private window<flags_chunk, hdf5_flags_writer>
{
private:
    friend class window<flags_chunk, hdf5_flags_writer>;

    const unit_system<std::int64_t, units::bytes, units::heaps::time, units::slices::time, units::chunks::time> time_sys;
    const unit_system<std::int64_t, units::heaps::freq, units::slices::freq, units::chunks::freq> freq_sys;
    const unit_system<std::int64_t, units::spectra, units::heaps::time, units::slices::time, units::chunks::time> timestamp_sys;
    const q::bytes chunk_bytes;
    q::slices_t n_slices{0};    ///< Total slices seen (including skipped ones)
    H5::DataSet dataset;

    static q::slices compute_chunk_size_slices(q::heaps heaps_per_slice);
    static q::heaps compute_chunk_size_heaps(q::heaps heaps_per_slice);
    void flush(flags_chunk &chunk);
public:
    hdf5_flags_writer(H5::Group &parent, int heaps_per_slice_freq,
                      int spectra_per_heap, int heaps_per_slice_time,
                      const char *name);
    ~hdf5_flags_writer();
    void add(const slice &s);
};

class hdf5_writer
{
private:
    q::ticks past_end_timestamp{-1};
    H5::H5File file;
    H5::Group group;
    hdf5_bf_raw_writer bf_raw;
    hdf5_timestamps_writer captured_timestamps, all_timestamps;
    hdf5_flags_writer flags;
    unit_system<std::int64_t, units::ticks, units::spectra, units::heaps::time, units::slices::time> timestamp_sys;

    static H5::FileAccPropList make_fapl(bool direct);

public:
    hdf5_writer(const std::string &filename, bool direct,
                int channels_per_heap, int heaps_per_slice_freq,
                std::int64_t ticks_between_spectra,
                int spectra_per_heap, int heaps_per_slice_time);
    void add(const slice &s);
    int get_fd() const;
};

#endif // COMMON_H

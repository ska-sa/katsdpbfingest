#ifndef WRITER_H
#define WRITER_H

#include <cstddef>
#include <H5Cpp.h>
#include "common.h"

class hdf5_bf_raw_writer
{
private:
    units::freq_system freq_sys;
    units::time_system time_sys;
    std::size_t chunk_bytes;
    H5::DataSet dataset;

public:
    hdf5_bf_raw_writer(H5::Group &parent,
                       const units::freq_system &freq_sys,
                       const units::time_system &time_sys,
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
    const units::time_system time_sys;

    void flush();
public:

    hdf5_timestamps_writer(H5::Group &parent, const units::time_system &time_sys,
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

    const unit_system<std::int64_t, units::channels, units::heaps::freq, units::slices::freq, units::chunks::freq> freq_sys;
    const unit_system<std::int64_t, units::ticks, units::spectra, units::heaps::time, units::slices::time, units::chunks::time> time_sys;
    const std::size_t chunk_bytes;
    q::slices_t n_slices{0};    ///< Total slices seen (including skipped ones)
    H5::DataSet dataset;

    static q::heaps heaps_per_slice(
        const units::freq_system &freq_sys, const units::time_system &time_sys);
    static q::slices compute_chunk_size_slices(
        const units::freq_system &freq_sys, const units::time_system &time_sys);
    static q::heaps compute_chunk_size_heaps(
        const units::freq_system &freq_sys, const units::time_system &time_sys);
    static std::size_t bytes(q::heaps n);

    void flush(flags_chunk &chunk);
public:
    hdf5_flags_writer(H5::Group &parent,
                      const units::freq_system &freq_sys,
                      const units::time_system &time_sys,
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
    const units::freq_system freq_sys;
    const units::time_system time_sys;
    hdf5_bf_raw_writer bf_raw;
    hdf5_timestamps_writer timestamps;
    hdf5_flags_writer flags;

    static H5::FileAccPropList make_fapl(bool direct);

public:
    hdf5_writer(const std::string &filename, bool direct,
                const units::freq_system &freq_sys,
                const units::time_system &time_sys);
    void add(const slice &s);
    int get_fd() const;
};

#endif // COMMON_H

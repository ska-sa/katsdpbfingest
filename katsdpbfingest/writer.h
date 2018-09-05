#ifndef WRITER_H
#define WRITER_H

#include <cstddef>
#include <H5Cpp.h>
#include "common.h"

class hdf5_bf_raw_writer
{
private:
    const int channels;
    const int spectra_per_heap;
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;

public:
    hdf5_bf_raw_writer(H5::Group &parent, int channels,
                       int spectra_per_heap,
                       const char *name);

    void add(const slice &c);
};

class hdf5_timestamps_writer
{
private:
    static constexpr hsize_t chunk = 1048576;
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;
    std::unique_ptr<std::uint64_t[], free_delete<std::uint64_t>> buffer;
    hsize_t n_buffer = 0;
    hsize_t n_written = 0;

    void flush();
public:
    const int spectra_per_heap;
    const std::uint64_t ticks_between_spectra;

    hdf5_timestamps_writer(H5::Group &parent, int spectra_per_heap,
                           std::uint64_t ticks_between_spectra, const char *name);
    ~hdf5_timestamps_writer();
    // Add a heap's worth of timestamps
    void add(std::uint64_t timestamp);
};

/**
 * Memory storage for an HDF5 chunk of flags data. This covers the whole band
 * and also many heaps in time.
 */
struct flags_chunk
{
    std::int64_t spectrum = -1;
    aligned_ptr<std::uint8_t> data;

    explicit flags_chunk(std::size_t size);
};

class hdf5_flags_writer : private window<flags_chunk, hdf5_flags_writer>
{
private:
    friend class window<flags_chunk, hdf5_flags_writer>;

    std::size_t spectra_per_heap;
    std::size_t heaps_per_slice;
    std::size_t heaps_per_chunk;
    std::size_t slices_per_chunk;
    hsize_t n_slices = 0;    ///< Total slices seen (including skipped ones)
    H5::DataSet dataset;
    H5::DSetMemXferPropList dxpl;

    static std::size_t compute_chunk_size(int heaps_per_slice);
    void flush(flags_chunk &chunk);
public:
    hdf5_flags_writer(H5::Group &parent, int heaps_per_slice, int spectra_per_heap,
                      const char *name);
    ~hdf5_flags_writer();
    void add(const slice &s);
};

class hdf5_writer
{
private:
    std::int64_t past_end_timestamp = -1;
    H5::H5File file;
    H5::Group group;
    hdf5_bf_raw_writer bf_raw;
    hdf5_timestamps_writer captured_timestamps, all_timestamps;
    hdf5_flags_writer flags;

    static H5::FileAccPropList make_fapl(bool direct);

public:
    hdf5_writer(const std::string &filename, bool direct,
                int channels, int channels_per_heap, int spectra_per_heap,
                std::int64_t ticks_between_spectra);
    void add(const slice &s);
    int get_fd() const;
};

#endif // COMMON_H

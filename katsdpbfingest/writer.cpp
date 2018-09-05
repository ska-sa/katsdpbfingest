#include <stdexcept>
#include <cstddef>
#include <cstring>
#include <cassert>
#include <H5Cpp.h>
#include "common.h"
#include "writer.h"

/**
 * Creates a dataset transfer property list that can be used for writing chunks
 * of size @a size. It is based on examining the code for H5DOwrite_chunk, but
 * doing it directly actually makes it easier to do things using the C++ API,
 * as well as avoiding the need to continually flip the flag on and off.
 */
static H5::DSetMemXferPropList make_dxpl_direct(std::size_t size)
{
    hbool_t direct_write = true;
    H5::DSetMemXferPropList dxpl;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_FLAG_NAME, &direct_write);
    // The size of this property changed somewhere between 1.8.11 and 1.8.17
    std::size_t property_size = dxpl.getPropSize(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME);
    if (property_size == sizeof(size))
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME, &size);
    else if (property_size == sizeof(std::uint32_t))
    {
        std::uint32_t size32 = size;
        assert(size32 == size);
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_DATASIZE_NAME, &size32);
    }
    return dxpl;
}

hdf5_bf_raw_writer::hdf5_bf_raw_writer(
    H5::Group &parent, int channels, int spectra_per_heap, const char *name)
    : channels(channels), spectra_per_heap(spectra_per_heap),
    dxpl(make_dxpl_direct(std::size_t(channels) * spectra_per_heap * 2))
{
    hsize_t dims[3] = {hsize_t(channels), 0, 2};
    hsize_t maxdims[3] = {hsize_t(channels), H5S_UNLIMITED, 2};
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_heap), 2};
    H5::DataSpace file_space(3, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(3, chunk);
    std::int8_t fill = 0;
    dcpl.setFillValue(H5::PredType::NATIVE_INT8, &fill);
    dataset = parent.createDataSet(name, H5::PredType::STD_I8BE, file_space, dcpl);
}

void hdf5_bf_raw_writer::add(const slice &s)
{
    hsize_t end = s.spectrum + spectra_per_heap;
    hsize_t new_size[3] = {hsize_t(channels), end, 2};
    dataset.extend(new_size);
    const hsize_t offset[3] = {0, hsize_t(s.spectrum), 0};
    const hsize_t *offset_ptr = offset;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
    dataset.write(s.data.get(), H5::PredType::STD_I8BE, H5::DataSpace::ALL, H5::DataSpace::ALL, dxpl);
}

constexpr hsize_t hdf5_timestamps_writer::chunk;

static void set_string_attribute(H5::H5Object &location, const std::string &name, const std::string &value)
{
    H5::DataSpace scalar;
    H5::StrType type(H5::PredType::C_S1, value.size());
    H5::Attribute attribute = location.createAttribute(name, type, scalar);
    attribute.write(type, value);
}

hdf5_timestamps_writer::hdf5_timestamps_writer(
    H5::Group &parent, int spectra_per_heap,
    std::uint64_t ticks_between_spectra, const char *name)
    : dxpl(make_dxpl_direct(chunk * sizeof(std::uint64_t))),
    spectra_per_heap(spectra_per_heap),
    ticks_between_spectra(ticks_between_spectra)
{
    hsize_t dims[1] = {0};
    hsize_t maxdims[1] = {H5S_UNLIMITED};
    H5::DataSpace file_space(1, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(1, &chunk);
    std::uint64_t fill = 0;
    dcpl.setFillValue(H5::PredType::NATIVE_UINT64, &fill);
    dataset = parent.createDataSet(
        name, H5::PredType::NATIVE_UINT64, file_space, dcpl);
    buffer = make_aligned<std::uint64_t>(chunk);
    n_buffer = 0;
    set_string_attribute(dataset, "timestamp_reference", "start");
    set_string_attribute(dataset, "timestamp_type", "adc");
}

hdf5_timestamps_writer::~hdf5_timestamps_writer()
{
    if (!std::uncaught_exception() && n_buffer > 0)
        flush();
}

void hdf5_timestamps_writer::flush()
{
    hsize_t new_size = n_written + n_buffer;
    dataset.extend(&new_size);
    const hsize_t offset[1] = {n_written};
    const hsize_t *offset_ptr = offset;
    dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
    if (n_buffer < chunk)
    {
        // Pad extra space with zeros - shouldn't matter, but this case
        // only arises when closing the file so should be cheap
        std::memset(buffer.get() + n_buffer, 0, (chunk - n_buffer) * sizeof(std::uint64_t));
    }
    dataset.write(buffer.get(), H5::PredType::NATIVE_UINT64, H5S_ALL, H5S_ALL, dxpl);
    n_written += n_buffer;
    n_buffer = 0;
}

void hdf5_timestamps_writer::add(std::uint64_t timestamp)
{
    for (int i = 0; i < spectra_per_heap; i++)
    {
        buffer[n_buffer++] = timestamp;
        timestamp += ticks_between_spectra;
    }
    assert(n_buffer <= chunk);
    if (n_buffer == chunk)
        flush();
}

static constexpr std::uint8_t data_lost = 1 << 3;

flags_chunk::flags_chunk(std::size_t size)
    : data(make_aligned<std::uint8_t>(size))
{
    std::memset(data.get(), data_lost, size);
}

std::size_t hdf5_flags_writer::compute_chunk_size(int heaps_per_slice)
{
    // Make each slice about 4MiB, rounding up if needed
    std::size_t slices = (4 * 1024 * 1024 + heaps_per_slice - 1) / heaps_per_slice;
    return slices * heaps_per_slice;
}

hdf5_flags_writer::hdf5_flags_writer(
    H5::Group &parent, int heaps_per_slice, int spectra_per_heap,
    const char *name)
    : window<flags_chunk, hdf5_flags_writer>(1, compute_chunk_size(heaps_per_slice)),
    spectra_per_heap(spectra_per_heap),
    heaps_per_slice(heaps_per_slice),
    heaps_per_chunk(compute_chunk_size(heaps_per_slice)),
    slices_per_chunk(heaps_per_chunk / heaps_per_slice),
    dxpl(make_dxpl_direct(heaps_per_chunk))
{
    hsize_t dims[2] = {hsize_t(heaps_per_slice), 0};
    hsize_t maxdims[2] = {dims[0], H5S_UNLIMITED};
    hsize_t chunk[2] = {dims[0], hsize_t(slices_per_chunk)};
    H5::DataSpace file_space(2, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(2, chunk);
    dcpl.setFillValue(H5::PredType::NATIVE_UINT8, &data_lost);
    dataset = parent.createDataSet(name, H5::PredType::STD_U8BE, file_space, dcpl);
}

hdf5_flags_writer::~hdf5_flags_writer()
{
    if (!std::uncaught_exception())
        flush_all();
}

void hdf5_flags_writer::flush(flags_chunk &chunk)
{
    if (chunk.spectrum != -1)
    {
        hsize_t new_size[2] = {hsize_t(heaps_per_slice), n_slices};
        dataset.extend(new_size);
        const hsize_t offset[2] = {0, hsize_t(chunk.spectrum / spectra_per_heap)};
        const hsize_t *offset_ptr = offset;
        dxpl.setProperty(H5D_XFER_DIRECT_CHUNK_WRITE_OFFSET_NAME, &offset_ptr);
        dataset.write(chunk.data.get(), H5::PredType::NATIVE_UINT8, H5S_ALL, H5S_ALL, dxpl);
    }
    chunk.spectrum = -1;
    std::memset(chunk.data.get(), data_lost, heaps_per_chunk);
}

void hdf5_flags_writer::add(const slice &s)
{
    std::int64_t slice_id = s.spectrum / spectra_per_heap;
    std::int64_t id = slice_id / slices_per_chunk;
    flags_chunk *chunk = get(id);
    assert(chunk != nullptr);  // we are given slices in-order, so cannot be behind the window
    std::size_t offset = slice_id - id * slices_per_chunk;
    for (std::size_t i = 0; i < s.present.size(); i++)
        chunk->data[i * slices_per_chunk + offset] = s.present[i] ? 0 : data_lost;
    chunk->spectrum = id * spectra_per_heap * slices_per_chunk;
    n_slices = slice_id + 1;
}

hdf5_writer::hdf5_writer(const std::string &filename, bool direct,
                         int channels, int channels_per_heap, int spectra_per_heap,
                         std::int64_t ticks_between_spectra)
    : file(filename, H5F_ACC_TRUNC, H5::FileCreatPropList::DEFAULT, make_fapl(direct)),
    group(file.createGroup("Data")),
    bf_raw(group, channels, spectra_per_heap, "bf_raw"),
    captured_timestamps(group, spectra_per_heap, ticks_between_spectra, "captured_timestamps"),
    all_timestamps(group, spectra_per_heap, ticks_between_spectra, "timestamps"),
    flags(group, channels / channels_per_heap, spectra_per_heap, "flags")
{
    H5::DataSpace scalar;
    // 1.8.11 doesn't have the right C++ wrapper for this to work, so we
    // duplicate its work
    hid_t attr_id = H5Acreate2(
        file.getId(), "version", H5::PredType::NATIVE_INT32.getId(),
        scalar.getId(), H5P_DEFAULT, H5P_DEFAULT);
    if (attr_id < 0)
        throw H5::AttributeIException("createAttribute", "H5Acreate2 failed");
    H5::Attribute version_attr(attr_id);
    /* Release the ref created by H5Acreate2 (version_attr has its own).
     * HDF5 1.8.11 has a bug where version_attr doesn't get its own
     * reference, so to handle both cases we have to check the current
     * value.
     */
    if (version_attr.getCounter() > 1)
        version_attr.decRefCount();
    const std::int32_t version = 3;
    version_attr.write(H5::PredType::NATIVE_INT32, &version);
}

H5::FileAccPropList hdf5_writer::make_fapl(bool direct)
{
    H5::FileAccPropList fapl;
    if (direct)
    {
#ifdef H5_HAVE_DIRECT
        if (H5Pset_fapl_direct(fapl.getId(), ALIGNMENT, ALIGNMENT, 128 * 1024) < 0)
            throw H5::PropListIException("hdf5_writer::make_fapl", "H5Pset_fapl_direct failed");
#else
        throw std::runtime_error("H5_HAVE_DIRECT not defined");
#endif
    }
    else
    {
        fapl.setSec2();
    }
    // Older versions of libhdf5 are missing the C++ version setLibverBounds
#ifdef H5F_LIBVER_18
    const auto version = H5F_LIBVER_18;
#else
    const auto version = H5F_LIBVER_LATEST;
#endif
    if (H5Pset_libver_bounds(fapl.getId(), version, version) < 0)
        throw H5::PropListIException("FileAccPropList::setLibverBounds", "H5Pset_libver_bounds failed");
    fapl.setAlignment(ALIGNMENT, ALIGNMENT);
    fapl.setFcloseDegree(H5F_CLOSE_SEMI);
    return fapl;
}

void hdf5_writer::add(const slice &s)
{
    if (past_end_timestamp == -1)
        past_end_timestamp = s.timestamp;
    while (past_end_timestamp <= s.timestamp)
    {
        all_timestamps.add(past_end_timestamp);
        past_end_timestamp += all_timestamps.ticks_between_spectra * all_timestamps.spectra_per_heap;
    }
    if (s.n_present == s.present.size())
        captured_timestamps.add(s.timestamp);
    bf_raw.add(s);
    flags.add(s);
}

int hdf5_writer::get_fd() const
{
    void *fd_ptr;
    file.getVFDHandle(&fd_ptr);
    return *reinterpret_cast<int *>(fd_ptr);
}

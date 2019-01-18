#include <stdexcept>
#include <cstddef>
#include <cstring>
#include <cassert>
#include <H5Cpp.h>
#include "common.h"
#include "writer.h"

// See write_direct below for explanation
#if !H5_VERSION_GE(1, 10, 3)
#include <H5DOpublic.h>
#endif

static void write_direct(
    H5::DataSet &dataset, const hsize_t *offset, q::bytes data_size, const void *buf)
{
#if H5_VERSION_GE(1, 10, 3)
    // 1.10.3 moved this functionality from H5DO into the core
    herr_t ret = H5Dwrite_chunk(dataset.getId(), H5P_DEFAULT, 0, offset, data_size.get(), buf);
#else
    herr_t ret = H5DOwrite_chunk(dataset.getId(), H5P_DEFAULT, 0, offset, data_size.get(), buf);
#endif
    if (ret < 0)
        throw H5::DataSetIException("DataSet::write_chunk", "H5Dwrite_chunk failed");
}

hdf5_bf_raw_writer::hdf5_bf_raw_writer(
    H5::Group &parent, int channels, int spectra_per_slice, const char *name)
    : freq_sys(channels),
    time_sys(2 * sizeof(std::int8_t), spectra_per_slice),
    chunk_bytes(time_sys.convert_one<units::slices::time, units::bytes>() * channels)
{
    hsize_t dims[3] = {hsize_t(channels), 0, 2};
    hsize_t maxdims[3] = {hsize_t(channels), H5S_UNLIMITED, 2};
    hsize_t chunk[3] = {hsize_t(channels), hsize_t(spectra_per_slice), 2};
    H5::DataSpace file_space(3, dims, maxdims);
    H5::DSetCreatPropList dcpl;
    dcpl.setChunk(3, chunk);
    std::int8_t fill = 0;
    dcpl.setFillValue(H5::PredType::NATIVE_INT8, &fill);
    dataset = parent.createDataSet(name, H5::PredType::STD_I8BE, file_space, dcpl);
}

void hdf5_bf_raw_writer::add(const slice &s)
{
    q::spectra end = s.spectrum + time_sys.convert_one<units::slices::time, units::spectra>();
    q::channels channels = freq_sys.convert_one<units::slices::freq, units::channels>();
    hsize_t new_size[3] = {hsize_t(channels.get()), hsize_t(end.get()), 2};
    dataset.extend(new_size);
    const hsize_t offset[3] = {0, hsize_t(s.spectrum.get()), 0};
    write_direct(dataset, offset, chunk_bytes, s.data.get());
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
    : timestamp_sys(ticks_between_spectra, spectra_per_heap)
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
    if (n_buffer < chunk)
    {
        // Pad extra space with zeros - shouldn't matter, but this case
        // only arises when closing the file so should be cheap.
        std::memset(buffer.get() + n_buffer, 0, (chunk - n_buffer) * sizeof(std::uint64_t));
    }
    write_direct(dataset, offset, chunk * q::bytes(sizeof(std::uint64_t)), buffer.get());
    n_written += n_buffer;
    n_buffer = 0;
}

void hdf5_timestamps_writer::add(q::ticks timestamp)
{
    q::spectra heap_spectra = timestamp_sys.convert_one<units::heaps::time, units::spectra>();
    for (q::spectra i{0}; i < heap_spectra; ++i)
        buffer[n_buffer++] = (timestamp + timestamp_sys.convert<units::ticks>(i)).get();
    assert(n_buffer <= chunk);
    if (n_buffer == chunk)
        flush();
}

flags_chunk::flags_chunk(q::heaps size)
    : data(make_aligned<std::uint8_t>(size.get()))
{
    std::memset(data.get(), data_lost, size.get() * sizeof(std::uint8_t));
}

q::slices hdf5_flags_writer::compute_chunk_size_slices(q::heaps heaps_per_slice)
{
    // Make each chunk about 4MiB, rounding up if needed
    std::size_t slices = (4 * 1024 * 1024 + heaps_per_slice.get() - 1) / heaps_per_slice.get();
    return q::slices(slices);
}

q::heaps hdf5_flags_writer::compute_chunk_size_heaps(q::heaps heaps_per_slice)
{
    return compute_chunk_size_slices(heaps_per_slice).get() * heaps_per_slice;
}

hdf5_flags_writer::hdf5_flags_writer(
    H5::Group &parent, int heaps_per_slice_freq,
    int spectra_per_heap, int heaps_per_slice_time,
    const char *name)
    : window<flags_chunk, hdf5_flags_writer>(
        1, compute_chunk_size_heaps(q::heaps(heaps_per_slice_freq * heaps_per_slice_time))),
    time_sys(sizeof(std::int8_t), heaps_per_slice_time,
             compute_chunk_size_slices(q::heaps(heaps_per_slice_freq * heaps_per_slice_time)).get()),
    freq_sys(heaps_per_slice_freq, 1),
    timestamp_sys(spectra_per_heap, heaps_per_slice_time,
                  time_sys.scale_factor<units::chunks::time, units::slices::time>()),
    chunk_bytes(
        freq_sys.scale_factor<units::chunks::freq, units::heaps::freq>()
        * time_sys.scale_factor<units::chunks::time, units::bytes>())
{
    hsize_t dims[2] = {
        hsize_t(freq_sys.scale_factor<units::chunks::freq, units::heaps::freq>()),
        0
    };
    hsize_t maxdims[2] = {dims[0], H5S_UNLIMITED};
    hsize_t chunk[2] = {
        hsize_t(freq_sys.scale_factor<units::chunks::freq, units::heaps::freq>()),
        hsize_t(time_sys.scale_factor<units::chunks::time, units::heaps::time>())
    };
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
    if (chunk.spectrum != q::spectra(-1))
    {
        hsize_t new_size[2] = {
            hsize_t(freq_sys.scale_factor<units::chunks::freq, units::heaps::freq>()),
            hsize_t(time_sys.convert<units::heaps::time>(n_slices).get())
        };
        dataset.extend(new_size);
        const hsize_t offset[2] = {
            0,
            hsize_t(timestamp_sys.convert_down<units::heaps::time>(chunk.spectrum).get())
        };
        write_direct(dataset, offset, chunk_bytes, chunk.data.get());
    }
    chunk.spectrum = q::spectra(-1);
    std::memset(chunk.data.get(), data_lost, chunk_bytes.get());
}

void hdf5_flags_writer::add(const slice &s)
{
    q::slices_t slice_id = timestamp_sys.convert_down<units::slices::time>(s.spectrum);
    q::chunks_t id = time_sys.convert_down<units::chunks::time>(slice_id);
    flags_chunk *chunk = get(id.get());
    assert(chunk != nullptr);  // we are given slices in-order, so cannot be behind the window
    // Note: code below doesn't yet allow for slice != chunk on frequency axis
    q::heaps_t offset = time_sys.convert<units::heaps::time>(slice_id)
        - time_sys.convert<units::heaps::time>(id);
    q::heaps_f slice_heaps_f = freq_sys.convert_one<units::slices::freq, units::heaps::freq>();
    q::heaps_t slice_heaps_t = time_sys.convert_one<units::slices::time, units::heaps::time>();
    q::heaps_t stride = time_sys.convert_one<units::chunks::time, units::heaps::time>();
    std::size_t present_idx = 0;
    for (q::heaps_f f{0}; f < slice_heaps_f; ++f)
        for (q::heaps_t t{0}; t < slice_heaps_t; ++t, present_idx++)
        {
            q::heaps pos = f * stride + (t + offset) * q::heaps_f(1);
            chunk->data[pos.get()] = s.present[present_idx] ? 0 : data_lost;
        }
    chunk->spectrum = timestamp_sys.convert<units::spectra>(id);
    n_slices = slice_id + q::slices_t(1);
}

hdf5_writer::hdf5_writer(const std::string &filename, bool direct,
                         int channels, int channels_per_heap,
                         int spectra_per_heap,
                         int heaps_per_slice_time,
                         std::int64_t ticks_between_spectra)
    : file(filename, H5F_ACC_TRUNC, H5::FileCreatPropList::DEFAULT, make_fapl(direct)),
    group(file.createGroup("Data")),
    bf_raw(group, channels, spectra_per_heap * heaps_per_slice_time, "bf_raw"),
    captured_timestamps(group, spectra_per_heap, ticks_between_spectra, "captured_timestamps"),
    all_timestamps(group, spectra_per_heap, ticks_between_spectra, "timestamps"),
    flags(group, channels / channels_per_heap, spectra_per_heap, heaps_per_slice_time, "flags")
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
    // Older versions of libhdf5 are missing the C++ version of setLibverBounds
#ifdef H5F_LIBVER_110
    const auto version = H5F_LIBVER_110;
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
    q::ticks timestamp{s.timestamp};
    if (past_end_timestamp == q::ticks(-1))
        past_end_timestamp = timestamp;
    while (past_end_timestamp <= timestamp)
    {
        all_timestamps.add(past_end_timestamp);
        past_end_timestamp += all_timestamps.timestamp_sys.convert_one<units::heaps::time, units::ticks>();
    }
    // TODO: this needs to look at the individual columns
    if (s.n_present == q::heaps(s.present.size()))
        captured_timestamps.add(timestamp);
    bf_raw.add(s);
    flags.add(s);
}

int hdf5_writer::get_fd() const
{
    void *fd_ptr;
    file.getVFDHandle(&fd_ptr);
    return *reinterpret_cast<int *>(fd_ptr);
}

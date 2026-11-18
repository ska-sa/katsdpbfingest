ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp
ARG KATSDPDOCKERBASE_TAG=latest

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build:$KATSDPDOCKERBASE_TAG AS build

# Build libhdf5 from source so that the direct I/O VFD can be used.
# The other flags are a subset of those used by debian.rules (subsetted
# mostly because the flags were default anyway), except that Fortran is
# disabled.
#
# The copy installed to /libhdf5-install is for the runtime image to copy from.
USER root

WORKDIR /tmp
# h5py 3.16 requires HDF5 >= 1.10.7. Stay within the existing 1.10 series.
ENV HDF5_VERSION=1.10.11
ARG KATSDPDOCKERBASE_MIRROR=http://sdp-services.kat.ac.za/mirror
RUN mirror_wget --progress=dot:mega https://support.hdfgroup.org/ftp/HDF5/current/src/hdf5-$HDF5_VERSION.tar.bz2 -O hdf5-$HDF5_VERSION.tar.bz2
RUN tar -jxf hdf5-$HDF5_VERSION.tar.bz2
WORKDIR /tmp/hdf5-$HDF5_VERSION
RUN ./configure --prefix=/usr/local --enable-build-mode=production --enable-threadsafe \
                --disable-fortran --enable-cxx --enable-direct-vfd \
                --enable-unsupported
RUN make -j4
RUN make DESTDIR=/libhdf5-install install
RUN make install
RUN ldconfig
RUN echo "Name: HDF5\nDescription: Hierarchical Data Format 5 (HDF5)\nVersion: $HDF5_VERSION\nRequires:\nCflags: -I/usr/local/include\nLibs: -L/usr/local/lib -lhdf5" \
        > /usr/lib/x86_64-linux-gnu/pkgconfig/hdf5.pc
RUN install -d -o kat -g kat /tmp/install
USER kat

# Install dependencies. We need to set library-dirs so that the new libhdf5
# will be found. We must avoid using the h5py wheel, because it will contain
# its own hdf5 libraries while we want to link to the system ones.

# Activate the base image virtualenv and make uv available.
ENV PATH="/home/kat/.local/bin:$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
WORKDIR /tmp/install
RUN printf '[build_ext]\nlibrary-dirs=/usr/local/lib\n' > setup.cfg

# Rebuild the inherited h5py against our HDF5, even if its version already
# matches the lock. Disable the cache so a wheel built against a different
# HDF5 installation cannot be reused. Binary policy belongs on installation,
# not just on the compile step.
RUN uv pip compile /tmp/install/requirements.txt \
        -c /home/kat/docker-base/base-requirements.lock \
        -o /tmp/install/requirements.lock && \
    HDF5_DIR=/usr/local uv pip install --no-cache --no-deps \
        --no-binary h5py --reinstall-package h5py \
        -r /tmp/install/requirements.lock && \
    uv pip check
RUN python -c 'import h5py, os; assert h5py.version.hdf5_version == os.environ["HDF5_VERSION"]; assert h5py.h5fd.DIRECT >= 0'

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpbfingest
WORKDIR /tmp/install/katsdpbfingest

RUN cp ../setup.cfg . && \
    uv pip install --no-deps . && \
    uv pip check
#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime:$KATSDPDOCKERBASE_TAG
LABEL maintainer="sdpdev+katsdpbfingest@ska.ac.za"

COPY --from=build /libhdf5-install /
USER root
RUN ldconfig
USER kat

COPY --from=build --chown=kat:kat /home/kat/ve3 /home/kat/ve3
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"

# Allow raw packets (for ibverbs raw QPs)
USER root
RUN setcap cap_net_raw+p /usr/local/bin/capambel
USER kat

EXPOSE 2050
EXPOSE 7148/udp

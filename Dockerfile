ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp
ARG TAG=uvpipfocal-fix

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build:$TAG as build

# Build libhdf5 from source so that the direct I/O VFD can be used.
# The other flags are a subset of those used by debian.rules (subsetted
# mostly because the flags were default anyway), except that Fortran is
# disabled.
#
# The copy installed to /libhdf5-install is for the runtime image to copy from.
USER root

WORKDIR /tmp
ENV HDF5_VERSION=1.10.3
ARG KATSDPDOCKERBASE_MIRROR=http://sdp-services.kat.ac.za/mirror
RUN mirror_wget https://s3.amazonaws.com/hdf-wordpress-1/wp-content/uploads/manual/HDF5/hdf5-$HDF5_VERSION.tar.bz2 -O hdf5-$HDF5_VERSION.tar.bz2
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
USER kat

# Install dependencies. We need to set library-dirs so that the new libhdf5
# will be found. We must avoid using the h5py wheel, because it will contain
# its own hdf5 libraries while we want to link to the system ones.
ENV PATH="$PATH_PYTHON3" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON3"
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
WORKDIR /tmp/install
RUN /bin/echo -e '[build_ext]\nlibrary-dirs=/usr/local/lib' > setup.cfg
# The base h5py pin is built against bundled HDF5. This image pins h5py to the
# version currently known to build against the custom HDF5 installed above.
RUN grep -v '^h5py==' ~/docker-base/base-requirements.txt > /tmp/install/base-requirements.txt && \
    sed '/^[[:space:]]*-[cd][[:space:]]/d; s/^h5py$/h5py==3.8.0/' \
        /tmp/install/requirements.txt > /tmp/install/requirements.in && \
    uv pip compile -c /tmp/install/base-requirements.txt /tmp/install/requirements.in -o /tmp/install/requirements.lock && \
    uv pip install --no-deps --no-binary h5py -r /tmp/install/requirements.lock && \
    uv pip check

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpbfingest
WORKDIR /tmp/install/katsdpbfingest
RUN cp ../setup.cfg .
RUN python ./setup.py clean
RUN uv pip install --no-deps .
RUN uv pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime:$TAG
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

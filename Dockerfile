ARG KATSDPDOCKERBASE_REGISTRY=harbor.sdp.kat.ac.za/dpp

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-build as build

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
RUN mirror_wget https://support.hdfgroup.org/ftp/HDF5/releases/hdf5-1.10/hdf5-$HDF5_VERSION/src/hdf5-$HDF5_VERSION.tar.bz2 -O hdf5-$HDF5_VERSION.tar.bz2
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
RUN install_pinned.py --no-binary=h5py -r /tmp/install/requirements.txt

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpbfingest
WORKDIR /tmp/install/katsdpbfingest
RUN cp ../setup.cfg .
RUN python ./setup.py clean
RUN pip install --no-deps .
RUN pip check

#######################################################################

FROM $KATSDPDOCKERBASE_REGISTRY/docker-base-runtime
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

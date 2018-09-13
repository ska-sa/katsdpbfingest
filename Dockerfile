FROM sdp-docker-registry.kat.ac.za:5000/docker-base-build as build
MAINTAINER Bruce Merry "bmerry@ska.ac.za"

# Build libhdf5 from source so that the direct I/O VFD can be used.
# The other flags are a subset of those used by debian.rules (subsetted
# mostly because the flags were default anyway), except that Fortran is
# disabled.
#
# The copy installed to /libhdf5-install is for the runtime image to copy from.
USER root

WORKDIR /tmp
ENV HDF5_VERSION=1.10.3
RUN wget "http://sdp-services.kat.ac.za/mirror/www.hdfgroup.org/package/source-bzip/index.html%3Fwpdmdl=12594" -O hdf5-$HDF5_VERSION.tar.bz2
RUN tar -jxf hdf5-$HDF5_VERSION.tar.bz2
WORKDIR /tmp/hdf5-$HDF5_VERSION
RUN ./configure --prefix=/usr/local --enable-build-mode=production --enable-threadsafe \
                --disable-fortran --enable-cxx --enable-direct-vfd \
                --enable-unsupported
RUN make -j4
RUN make DESTDIR=/libhdf5-install install
RUN make install
RUN ldconfig
RUN echo -e "Name: HDF5\nDescription: Hierarchical Data Format 5 (HDF5)\nVersion: $HDF5_VERSION\nRequires:\nCflags: -I/usr/local/include\nLibs: -L/usr/local/lib -lhdf5" \
        > /usr/lib/x86_64-linux-gnu/pkgconfig/hdf5.pc
USER kat

# Install dependencies. We need to set library-dirs so that the new libhdf5
# will be found. We must avoid using the h5py wheel, because it will contain
# its own hdf5 libraries while we want to link to the system ones.
ENV PATH="$PATH_PYTHON2" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON2"
COPY --chown=kat:kat requirements.txt /tmp/install/requirements.txt
WORKDIR /tmp/install
RUN /bin/echo -e '[build_ext]\nlibrary-dirs=/usr/local/lib' > setup.cfg
RUN install-requirements.py --no-binary=h5py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY --chown=kat:kat . /tmp/install/katsdpbfingest
WORKDIR /tmp/install/katsdpbfingest
RUN cp ../setup.cfg .
RUN python ./setup.py clean
RUN pip install --no-deps .
RUN pip check

#######################################################################

FROM sdp-docker-registry.kat.ac.za:5000/docker-base-runtime
MAINTAINER Bruce Merry "bmerry@ska.ac.za"

COPY --from=build /libhdf5-install /
USER root
RUN ldconfig
USER kat

COPY --from=build --chown=kat:kat /home/kat/ve /home/kat/ve
ENV PATH="$PATH_PYTHON2" VIRTUAL_ENV="$VIRTUAL_ENV_PYTHON2"

EXPOSE 2050
EXPOSE 7148/udp

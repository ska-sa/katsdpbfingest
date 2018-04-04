FROM sdp-docker-registry.kat.ac.za:5000/docker-base

MAINTAINER Bruce Merry "bmerry@ska.ac.za"

# Install libhdf5 from source so that the direct I/O VFD can be used.
# 1.8.17 also fixes a resource leak bug (reported as an infinite loop on
# shutdown) that affects the version shipped with Ubuntu 14.04.
# The other flags are a subset of those used by debian.rules (subsetted
# mostly because the flags were default anyway).
#
# Also remove the wheel built for h5py, because it was linked against the
# system version and needs to be recompiled
USER root
RUN cd /tmp && \
    wget http://sdp-services.kat.ac.za/mirror/support.hdfgroup.org/ftp/HDF5/current/src/hdf5-1.8.17.tar.bz2 && \
    tar -jxf hdf5-1.8.17.tar.bz2 && \
    cd hdf5-1.8.17 && \
    ./configure --prefix=/usr/local --enable-production --enable-threadsafe \
                --enable-fortran --enable-cxx --enable-direct-vfd \
                --enable-unsupported && \
    make -j4 && \
    make install && \
    ldconfig && \
    rm -rf /tmp/libhdf5-1.8.17 && \
    echo -e 'Name: HDF5\nDescription: Hierarchical Data Format 5 (HDF5)\nVersion: 1.8.17\nRequires:\nCflags: -I/usr/local/include\nLibs: -L/usr/local/lib -lhdf5' \
        > /usr/lib/x86_64-linux-gnu/pkgconfig/hdf5.pc && \
    find /home/kat/.cache/pip/wheels -name 'h5py-*.whl' -exec rm -- '{}' ';'
USER kat

# Install dependencies. We need to set library-dirs so that the new libhdf5
# will be found. And we need to create /tmp/install and
# /tmp/install/katsdpbfingest as user kat so that we can write to it later.
RUN mkdir -p /tmp/install/katsdpbfingest
COPY requirements.txt /tmp/install/requirements.txt
RUN cd /tmp/install && \
    /bin/echo -e '[build_ext]\nlibrary-dirs=/usr/local/lib' > setup.cfg && \
    install-requirements.py -d ~/docker-base/base-requirements.txt -r /tmp/install/requirements.txt

# Install the current package
COPY . /tmp/install/katsdpbfingest
RUN cd /tmp/install/katsdpbfingest && cp ../setup.cfg . && \
    python ./setup.py clean && pip install --no-deps . && pip check

EXPOSE 2050
EXPOSE 7148/udp

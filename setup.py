#!/usr/bin/env python3
from setuptools import setup, find_packages, Extension
from distutils.command.build_ext import build_ext     # type: ignore  # typeshed doesn't capture it
import glob
import importlib
import subprocess
import os.path
try:
    import pkgconfig
    hdf5 = pkgconfig.parse('hdf5')
except ImportError:
    import collections
    hdf5 = collections.defaultdict(list)


tests_require = ['nose', 'spead2>=1.11.1', 'asynctest']


class get_include:
    """Helper class to defer importing a module until build time for fetching
    the include directory.
    """
    def __init__(self, module, *args, **kwargs):
        self.module = module
        self.args = args
        self.kwargs = kwargs

    def __str__(self):
        module = importlib.import_module(self.module)
        return getattr(module, 'get_include')(*self.args, **self.kwargs)


# Hack: this is copied and edited from spead2, so that we can run configure
# inside the spead2 submodule.
class BuildExt(build_ext):
    def run(self):
        self.mkpath(self.build_temp)
        subprocess.check_call(['./bootstrap.sh', '--no-python'], cwd='spead2')
        subprocess.check_call(os.path.abspath('spead2/configure'), cwd=self.build_temp)
        # Ugly hack to add libraries conditional on configure result
        have_ibv = False
        have_pcap = False
        with open(os.path.join(self.build_temp, 'include', 'spead2', 'common_features.h')) as f:
            for line in f:
                if line.strip() == '#define SPEAD2_USE_IBV 1':
                    have_ibv = True
                elif line.strip() == '#define SPEAD2_USE_PCAP 1':
                    have_pcap = True
        for extension in self.extensions:
            if have_ibv:
                extension.libraries.extend(['rdmacm', 'ibverbs'])
            if have_pcap:
                extension.libraries.extend(['pcap'])
            extension.include_dirs.insert(0, os.path.join(self.build_temp, 'include'))
        # distutils uses old-style classes, so no super
        build_ext.run(self)


extensions = [
    Extension(
        '_bf_ingest',
        sources=(glob.glob('spead2/src/common_*.cpp') +
                 glob.glob('spead2/src/recv_*.cpp') +
                 glob.glob('spead2/src/send_*.cpp') +
                 glob.glob('spead2/src/py_common.cpp') +
                 glob.glob('katsdpbfingest/*.cpp')),
        depends=(glob.glob('spead2/include/spead2/*.h') +
                 glob.glob('spead2/3rdparty/pybind11/include/pybind11/*.h') +
                 glob.glob('spead2/3rdparty/pybind11/include/pybind11/detail/*.h') +
                 glob.glob('katsdpbfingest/*.h')),
        language='c++',
        include_dirs=['spead2/include', 'spead2/3rdparty/pybind11/include'] + hdf5['include_dirs'],
        define_macros=hdf5['define_macros'],
        extra_compile_args=['-std=c++14', '-g0', '-O3', '-fvisibility=hidden'],
        library_dirs=hdf5['library_dirs'],
        # libgcc needs to be explicitly linked for multi-function versioning
        libraries=['boost_system', 'hdf5_cpp', 'hdf5_hl', 'gcc'] + hdf5['libraries']
    )
]

setup(
    name="katsdpbfingest",
    description="Karoo Array Telescope Data Capture",
    author="Bruce Merry",
    author_email="bmerry@ska.ac.za",
    packages=find_packages(),
    ext_package='katsdpbfingest',
    ext_modules=extensions,
    cmdclass={'build_ext': BuildExt},
    scripts=["scripts/bf_ingest.py"],
    setup_requires=['katversion', 'pkgconfig'],
    install_requires=[
        'h5py',
        'numpy',
        'aiokatcp',
        'katsdpservices',
        'katsdptelstate',
        'spead2'
    ],
    extras_require={
        'test': tests_require
    },
    tests_require=tests_require,
    use_katversion=True
)

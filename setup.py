#!/usr/bin/env python3
from setuptools import setup, find_packages, Extension
from distutils.command.build_ext import build_ext     # type: ignore  # typeshed doesn't capture it
import configparser
import glob
import subprocess
import os.path

try:
    import pybind11
    import pkgconfig
    hdf5 = pkgconfig.parse('hdf5')
    pybind11_include = pybind11.get_include()
except ImportError:
    # Just to make "./setup.py clean" work
    import collections
    hdf5 = collections.defaultdict(list)
    pybind11_include = ''


tests_require = ['nose', 'spead2>=3.0.1', 'asynctest']


# Hack: this is copied and edited from spead2, so that we can run configure
# inside the spead2 submodule.
class BuildExt(build_ext):
    def run(self):
        self.mkpath(self.build_temp)
        subprocess.check_call(['./bootstrap.sh'], cwd='spead2')
        subprocess.check_call(os.path.abspath('spead2/configure'), cwd=self.build_temp)
        config = configparser.ConfigParser()
        config.read(os.path.join(self.build_temp, 'python-build.cfg'))
        for extension in self.extensions:
            extension.extra_compile_args.extend(config['compiler']['CFLAGS'].split())
            extension.extra_link_args.extend(config['compiler']['LIBS'].split())
            extension.include_dirs.insert(0, os.path.join(self.build_temp, 'include'))
        super().run()


sources = (glob.glob('spead2/src/common_*.cpp') +
           glob.glob('spead2/src/recv_*.cpp') +
           glob.glob('spead2/src/send_*.cpp') +
           glob.glob('spead2/src/py_common.cpp') +
           glob.glob('katsdpbfingest/*.cpp'))
# Generated files: might be missing from sources
gen_sources = [
    'spead2/src/common_loader_ibv.cpp',
    'spead2/src/common_loader_rdmacm.cpp',
    'spead2/src/common_loader_mlx5dv.cpp'
]
for source in gen_sources:
    if source not in sources:
        sources.append(source)
extensions = [
    Extension(
        '_bf_ingest',
        sources=sources,
        depends=(glob.glob('spead2/include/spead2/*.h') +
                 glob.glob('katsdpbfingest/*.h')),
        language='c++',
        include_dirs=['spead2/include', pybind11_include] + hdf5['include_dirs'],
        define_macros=hdf5['define_macros'],
        extra_compile_args=['-std=c++14', '-g0', '-O3', '-fvisibility=hidden'],
        library_dirs=hdf5['library_dirs'],
        # libgcc needs to be explicitly linked for multi-function versioning
        libraries=['boost_system', 'hdf5_cpp', 'hdf5_hl', 'gcc'] + hdf5['libraries']
    )
]

setup(
    name="katsdpbfingest",
    description="MeerKAT beamformer data capture",
    author="MeerKAT SDP team",
    author_email="sdpdev+katsdpbfingest@ska.ac.za",
    packages=find_packages(),
    ext_package='katsdpbfingest',
    ext_modules=extensions,
    cmdclass={'build_ext': BuildExt},
    scripts=["scripts/bf_ingest.py"],
    install_requires=[
        'h5py',
        'numpy',
        'aiokatcp',
        'katsdpservices[argparse,aiomonitor]',
        'katsdptelstate >= 0.10',
        'spead2 >= 3.0.1'
    ],
    extras_require={
        'test': tests_require
    },
    tests_require=tests_require,
    use_katversion=True
)

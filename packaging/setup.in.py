#!/usr/bin/env python
# (c)quasardb SAS. All rights reserved.
# qdb is a trademark of quasardb SAS

# pylint: disable=C0103,C0111,C0326,W0201

from builtins import range as xrange  # pylint: disable=W0622

import os
import re
import sys
import platform
import subprocess

import glob
import shutil

from setuptools.command.build_ext import build_ext
from distutils.version import LooseVersion

from setuptools import setup, Extension
from setuptools.command.bdist_egg import bdist_egg as old_bdist_egg  # pylint: disable=C0412
from pkg_resources import get_build_platform

import ez_setup
ez_setup.use_setuptools()

qdb_version = "@QDB_PY_VERSION@".lower()

is_clang = 'Clang' in '@CMAKE_CXX_COMPILER_ID@'
is_windows = os.name == 'nt'
is_freebsd = sys.platform.startswith('freebsd')
is_linux = sys.platform.startswith('linux')
is_osx = sys.platform == 'darwin'
is_64_bits = sys.maxsize > 2**32
arch = "x64" if is_64_bits else "x86"

# get the additional libraries for the package
package_modules = glob.glob(os.path.join('quasardb', '@QDB_PYTHON_LIBRARY_GLOB@'))
if is_osx:
    package_modules.extend(glob.glob(os.path.join('quasardb', '@SHARED_LIBRARY_EXTENSIONS@')))

package_name = 'quasardb'

class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)

class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError:
            raise RuntimeError("CMake must be installed to build the following extensions: " +
                               ", ".join(e.name for e in self.extensions))

        if platform.system() == "Windows":
            cmake_version = LooseVersion(re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise RuntimeError("CMake >= 3.1.0 is required on Windows")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.join(os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name))), 'quasardb')
        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(cfg.upper(), extdir)]
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
        #    build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
       #     build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(env.get('CXXFLAGS', ''),
                                                              self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] + cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] + build_args, cwd=self.build_temp)

class EggRetagger(old_bdist_egg):
    def finalize_options(self):
        if self.plat_name is None:
            self.plat_name = get_build_platform()

        if self.plat_name.startswith('freebsd'):
            parts = self.plat_name.split('-')
            self.plat_name = parts[0] + '-' + \
                parts[1].split('.')[0] + '-' + parts[-1]

        old_bdist_egg.finalize_options(self)

from wheel.bdist_wheel import bdist_wheel as old_bdist_wheel

class WheelRetagger(old_bdist_wheel):
    def get_tag(self):
        tag = old_bdist_wheel.get_tag(self)

        python_tag = tag[0]
        abi_tag = tag[1]
        platform_tag = tag[2]

        if platform_tag.startswith('freebsd'):
            parts = platform_tag.split('_')
            platform_tag = '_'.join(parts[:2]) + '_' + parts[-1]

        if platform_tag.startswith('macosx_10_') and platform_tag.endswith('_x86_64'):
            supported_versions = [
                'macosx_10_6', 'macosx_10_9', 'macosx_10_10', 'macosx_10_11', 'macosx_10_12']
            for i in xrange(len(supported_versions)):
                supported_versions[i] = supported_versions[i] + '_x86_64'
            platform_tag = '.'.join(supported_versions)

        tag = (python_tag, abi_tag, platform_tag)
        return tag

cmdclass = {
    'build_ext': CMakeBuild,
    'bdist_egg': EggRetagger,
    'bdist_wheel': WheelRetagger,
}


setup(name=package_name,
      version=qdb_version,
      description='Python API for quasardb',
      author='quasardb SAS',
      license='BSD',
      author_email='contact@quasardb.net',
      url='https://www.quasardb.net/',
      classifiers=[
          'Development Status :: 5 - Production/Stable',
          'Intended Audience :: End Users/Desktop',
          'Intended Audience :: Financial and Insurance Industry',
          'Intended Audience :: Information Technology',
          'Intended Audience :: Other Audience',
          'Intended Audience :: System Administrators',
          'Intended Audience :: Telecommunications Industry',
          'Programming Language :: Python :: 2.7',
          'Programming Language :: Python :: 3.6',
          'Topic :: Database',
          'Topic :: Software Development :: Libraries :: Python Modules',
          "License :: OSI Approved :: BSD License",
      ],
      keywords='quasardb timeseries database API driver ',
      setup_requires=["setuptools_git >= 0.3", "xmlrunner", "future", "numpy", "wheel"],
      install_requires=["xmlrunner", "future", "numpy", "wheel"],
      packages=[package_name],
      package_data={package_name: package_modules},
      ext_modules= [CMakeExtension('quasardb', 'quasardb_module')],
      include_package_data=True,
      cmdclass=cmdclass,
      zip_safe=False,
     )
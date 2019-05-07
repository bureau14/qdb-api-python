#!/usr/bin/env python
# (c)quasardb SAS. All rights reserved.
# qdb is a trademark of quasardb SAS

# pylint: disable=C0103,C0111,C0326,W0201


import os
import re
import sys
import platform
import subprocess
import glob

from distutils.version import LooseVersion
from setuptools.command.build_ext import build_ext

from setuptools import setup, Extension
from setuptools.command.bdist_egg import bdist_egg as old_bdist_egg  # pylint: disable=C0412
from pkg_resources import get_build_platform
from wheel.bdist_wheel import bdist_wheel as old_bdist_wheel

qdb_version = "3.4.0.dev0".lower()

# package_modules are our 'extra' files. Our cmake configuration copies our QDB_API_LIB
# into our source directory, and by adding this to `package_modules` we tell setuptools to
# package this.
package_modules = glob.glob(os.path.join('quasardb', 'lib*'))
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
            cmake_version = LooseVersion(
                re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise RuntimeError("CMake >= 3.1.0 is required on Windows")

        for ext in self.extensions:
            self.build_extension(ext)

    def build_extension(self, ext):
        extdir = os.path.join(
            os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name))), 'quasardb')

        # We provide CMAKE_LIBRARY_OUTPUT_DIRECTORY to cmake, where it will copy libqdb_api.so (or
        # whatever the OS uses). It is important that this path matches `package_modules`, so that
        # setuptools knows it needs to package this.
        cmake_args = ['-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
                      '-DPYTHON_EXECUTABLE=' + sys.executable,
                      '-DQDB_PY_VERSION=' + qdb_version]

        cfg = 'Debug' if self.debug else 'Release'
        build_args = ['--config', cfg]

        if platform.system() == "Windows":
            cmake_args += [
                '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY_{}={}'.format(cfg.upper(), extdir)]
            cmake_args += ['-T', 'host=x64']
            if sys.maxsize > 2**32:
                cmake_args += ['-A', 'x64']
            else:
                cmake_args += ['-A', 'Win32']
        #    build_args += ['--', '/m']
        else:
            cmake_args += ['-DCMAKE_BUILD_TYPE=' + cfg]
        #    build_args += ['--', '-j2']

        env = os.environ.copy()
        env['CXXFLAGS'] = '{} -DVERSION_INFO=\\"{}\\"'.format(env.get('CXXFLAGS', ''),
                                                              self.distribution.get_version())
        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)
        subprocess.check_call(['cmake', ext.sourcedir] +
                              cmake_args, cwd=self.build_temp, env=env)
        subprocess.check_call(['cmake', '--build', '.'] +
                              build_args, cwd=self.build_temp)

# Generated by CMake


class EggRetagger(old_bdist_egg):
    def finalize_options(self):
        if self.plat_name is None:
            self.plat_name = get_build_platform()

        old_bdist_egg.finalize_options(self)


# Generated by CMake


class WheelRetagger(old_bdist_wheel):
    def get_tag(self):
        tag = old_bdist_wheel.get_tag(self)

        python_tag = tag[0]
        abi_tag = tag[1]
        platform_tag = tag[2]

        if platform_tag.startswith('macosx_10_') and platform_tag.endswith('_x86_64'):
            supported_versions = [
                'macosx_10_6', 'macosx_10_9', 'macosx_10_10', 'macosx_10_11', 'macosx_10_12']
            supported_versions = [
                version + '_x86_64' for version in supported_versions]
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
      setup_requires=[
          "setuptools_git >= 0.3",
          "xmlrunner >= 1.7.7",
          "future >= 0.17.1",
          "numpy >= 1.16.1",
          "pytest >= 4.2.0",
          "pytest-runner >= 4.2",
          "pytz >= 2018.9",
          "pandas >= 0.24.0",
          "pytest-benchmark >= 3.2.2"],
      install_requires=[
          "xmlrunner >= 1.7.7",
          "future >= 0.17.1",
          "numpy >= 1.16.1"],
      packages=[package_name],
      package_data={package_name: package_modules},
      ext_modules=[CMakeExtension('quasardb', 'quasardb/')],
      include_package_data=True,
      cmdclass=cmdclass,
      zip_safe=False,
      test_suite="tests",
      )

#!/usr/bin/env python
# (c)quasardb SAS. All rights reserved.
# qdb is a trademark of quasardb SAS

# pylint: disable=C0103,C0111,C0326,W0201,line-too-long


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
from setuptools.command.install import install
# NOTE: Import distutils after setuptools.
from pkg_resources import get_build_platform
from wheel.bdist_wheel import bdist_wheel as old_bdist_wheel

qdb_version = "3.13.5.post2"

# package_modules are our 'extra' files. Our cmake configuration copies our QDB_API_LIB
# into our source directory, and by adding this to `package_modules` we tell setuptools to
# package this.
package_modules = glob.glob(os.path.join('quasardb', 'lib*'))
package_name = 'quasardb'
packages = [
    package_name,
    "quasardb.pandas",
    "quasardb.numpy",
    "quasardb.extensions",
    ]


class CMakeExtension(Extension):
    def __init__(self, name, sourcedir=''):
        Extension.__init__(self, name, sources=[])
        self.sourcedir = os.path.abspath(sourcedir)


class CMakeBuild(build_ext):
    def run(self):
        try:
            out = subprocess.check_output(['cmake', '--version'])
        except OSError as ex:
            raise RuntimeError("CMake must be installed to build the following extensions: " +
                               ", ".join(e.name for e in self.extensions)) from ex

        if platform.system() == "Windows":
            cmake_version = LooseVersion(
                re.search(r'version\s*([\d.]+)', out.decode()).group(1))
            if cmake_version < '3.1.0':
                raise RuntimeError("CMake >= 3.1.0 is required on Windows")

        for ext in self.extensions:
            self.build_extension(ext)

    def find_osx_sysroot(self):
        # Override sysroot, because otherwise bdist (setuptools) will set it to the latest SDK.
        # Cf. https://github.com/pypa/setuptools/blob/eb75ea6eb827acf1be6c350850b350de7b500efd/setuptools/_distutils/unixccompiler.py#L326-L350 # noqa

        # Paths can be different depending on XCode version installed:
        # /Library/Developer/CommandLineTools/SDKs/MacOSX11.sdk
        # /Applications/Xcode.app/Contents/Developer/Platforms/MacOSX.platform/Developer/SDKs/MacOSX11.sdk
        sdk = '10.14'
        out = subprocess.check_output(
            ['xcrun', '--sdk', 'macosx' + sdk, '--show-sdk-path'])
        print(out.decode().strip())
        return out.decode().strip()

    def build_extension(self, ext):
        extdir = os.path.join(
            os.path.abspath(os.path.dirname(self.get_ext_fullpath(ext.name))), 'quasardb')

        # We provide CMAKE_LIBRARY_OUTPUT_DIRECTORY to cmake, where it will copy libqdb_api.so (or
        # whatever the OS uses). It is important that this path matches `package_modules`, so that
        # setuptools knows it needs to package this.
        cmake_args = [
            '-DCMAKE_LIBRARY_OUTPUT_DIRECTORY=' + extdir,
            '-DPYTHON_EXECUTABLE=' + sys.executable,
            '-DQDB_PY_VERSION=' + qdb_version,
        ]

        if platform.system() == "Darwin":
            cmake_args += ['-DCMAKE_OSX_SYSROOT:PATH=' + self.find_osx_sysroot()]

        # If certain environment variables are set, we pass them along
        # to cmake. This allows for greater flexibility for an end-user
        # to configure builds.
        proxied_env_vars = {'CMAKE_GENERATOR': ['-G', '{}'],
                            'CMAKE_C_COMPILER': ['-D', 'CMAKE_C_COMPILER={}'],
                            'CMAKE_CXX_COMPILER': ['-D', 'CMAKE_CXX_COMPILER={}'],
                            'QDB_LINKER': ['-D', 'QDB_LINKER={}'],
                            'CMAKE_BUILD_TYPE': ['-D', 'CMAKE_BUILD_TYPE={}'],
                            'CMAKE_OSX_DEPLOYMENT_TARGET': ['-D', 'CMAKE_OSX_DEPLOYMENT_TARGET={}'],
                            'CMAKE_OSX_SYSROOT': ['-D', 'CMAKE_OSX_SYSROOT={}'],
                            'CMAKE_VERBOSE_MAKEFILE': ['-D', 'CMAKE_VERBOSE_MAKEFILE={}'],
                            }
        default_proxy_vals = {'CMAKE_BUILD_TYPE': 'Release'}

        for (env_var, cmake_args_) in proxied_env_vars.items():
            default_ = default_proxy_vals.get(env_var, 0)
            value = os.environ.get(env_var, default_)

            if value:
                cmake_args_[-1] = cmake_args_[-1].format(value)
                print("Proxying CMake args: {}".format(cmake_args_))
                cmake_args += [*cmake_args_]

        # NOTE: Run `python setup.py build_ext --debug bdist_wheel ...` for Debug build.
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
        elif platform.system() == "Linux":
            build_args += ['-j']
        #    build_args += ['--', '-j2']

        env = os.environ.copy()

        additional_cmake_params = env.get('ADDITIONAL_CMAKE_PARAMETERS')
        if additional_cmake_params:
            additional_cmake_params = additional_cmake_params.split(':')
            cmake_args += additional_cmake_params

        c_cxx_flags = [
            '-DVERSION_INFO=\\"{}\\"'.format(self.distribution.get_version()),
        ]

        joined_c_cxx_flags = ' '.join(c_cxx_flags)
        env['CFLAGS']   = '{} {}'.format(env.get('CFLAGS', ''),   joined_c_cxx_flags)
        env['CXXFLAGS'] = '{} {}'.format(env.get('CXXFLAGS', ''), joined_c_cxx_flags)

        if not os.path.exists(self.build_temp):
            os.makedirs(self.build_temp)

        config_call = ['cmake', ext.sourcedir] + cmake_args
        build_call  = ['cmake', '--build', '.'] + build_args

        subprocess.check_call(config_call, cwd=self.build_temp, env=env)
        subprocess.check_call(build_call, cwd=self.build_temp)

# Generated by CMake
class EggRetagger(old_bdist_egg):
    def finalize_options(self):
        if self.plat_name is None:
            self.plat_name = get_build_platform()

        old_bdist_egg.finalize_options(self)

# Generated by CMake
class WheelRetagger(old_bdist_wheel):
    def finalize_options(self):
        super().finalize_options()
        # Mark us as a non-pure Python package.
        self.root_is_pure = False

    def get_tag(self):
        # Cf. https://github.com/benfred/py-spy/blob/00c37c3474faca85f2389eb73f9d02f7146fa567/setup.py # noqa
        # Cf. https://stackoverflow.com/questions/45150304/how-to-force-a-python-wheel-to-be-platform-specific-when-building-it # noqa
        py, abi, plat = old_bdist_wheel.get_tag(self)

        if platform.system() == "Darwin" and os.getenv('MACOSX_DEPLOYMENT_TARGET'):
            target = os.environ['MACOSX_DEPLOYMENT_TARGET']
            plat = "macosx_{}_{}".format(target.replace(".", "_"), platform.machine())

        return (py, abi, plat)


class InstallCommand(install):
    def run(self):
        if platform.system() == "Darwin":
            os.environ.setdefault("MACOSX_DEPLOYMENT_TARGET", "10.14")

        # run this after trying to build with cargo (as otherwise this leaves
        # venv in a bad state: https://github.com/benfred/py-spy/issues/69)
        install.run(self)


cmdclass = {
    'install': InstallCommand,
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
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Programming Language :: Python :: 3.8',
          'Programming Language :: Python :: 3.9',
          'Programming Language :: Python :: 3.10',
          'Topic :: Database',
          'Topic :: Software Development :: Libraries :: Python Modules',
          "License :: OSI Approved :: BSD License",
      ],
      keywords='quasardb timeseries database API driver ',
      setup_requires=["setuptools-git"],
      install_requires=["numpy"],
      extras_require={
          "pandas": ["pandas"],
          "tests": [
              "pytest >= 6.2.5",
              "pytest-runner >= 5.3.1",
              "pytest-benchmark == 3.4.1",
              "teamcity-messages >= 1.29"]
      },

      packages=packages,
      package_data={package_name: package_modules},
      ext_modules=[CMakeExtension('quasardb', 'quasardb/')],
      include_package_data=True,
      cmdclass=cmdclass,
      zip_safe=False,
      test_suite="tests",
      )

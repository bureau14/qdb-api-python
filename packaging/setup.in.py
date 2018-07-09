#!/usr/bin/env python
# (c)quasardb SAS. All rights reserved.
# qdb is a trademark of quasardb SAS

# pylint: disable=C0103,C0111,C0326,W0201

from builtins import range as xrange  # pylint: disable=W0622
import glob
import os
import sys
import shutil
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

pyd_file = glob.glob(os.path.join('@QDB_PYD_DIR@', '@QDB_PYD_EXT@'))

for pyd in pyd_file:
  shutil.copy(pyd, 'quasardb')

package_modules = glob.glob(os.path.join('quasardb', '@QDB_PYTHON_LIBRARY_GLOB@'))
if is_osx:
    package_modules.extend(glob.glob(os.path.join('quasardb', '@SHARED_LIBRARY_EXTENSIONS@')))

package_name = 'quasardb'

# we need to add this useless void module to force setup.py to tag this packaging
# as platform specific
nothing_module = Extension('quasardb.nothing', sources = [ 'nothing.c'])

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
      package_data={package_name: pyd_file + package_modules},
      ext_modules= [nothing_module],
      include_package_data=True,
      cmdclass=cmdclass,
      zip_safe=False,
     )

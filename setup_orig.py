#!/usr/bin/env python
# (c)Bureau 14 SARL. All rights reserved.
# qdb is a trademark of Bureau 14 SARL

from distutils.core import setup, Extension
import os.path
import glob
import sys

# get rid of useless -Wstrict-prototypes through this hack
# this is a bug in distutils
from distutils.sysconfig import get_config_vars

(opt,) = get_config_vars('OPT')
os.environ['OPT'] = " ".join(flag for flag in opt.split() if flag != '-Wstrict-prototypes')

''' parameters given by CMake '''
qdb_version = "@QDB_VERSION@"

# A python binary module is like a dynamic library, use the same linking flags.
extra_link_args = "@ADDITIONNAL_LINK_FLAGS@".strip().split(';')
extra_compile_args ="@QDB_PYTHON_COMPILE_FLAG@".split(';')

package_modules = glob.glob(os.path.join('qdb', '@QDB_PYTHON_LIBRARY_GLOB@'))

package_name = 'qdb-python-api-bin'

if len(sys.argv) > 1:
  if sys.argv[1] == 'sdist':
      package_name = 'qdb-python-api-src-@PY_PACKAGE_SOURCE_SUFFIX@-@QDB_CPU_ARCH@'

setup(name=package_name,
      version=qdb_version,
      description='Python API for the quasardb data engine software',
      author='Bureau 14 SARL',
      author_email='contact@bureau14.fr',
      url='http://www.quasardb.net/',
      packages=['qdb'],
      package_data={'qdb': [os.path.basename(mod) for mod in package_modules]},
      ext_modules=[Extension(
        'qdb._qdb', [os.path.join('src', '@QDB_PY_WRAPPER@')],
        include_dirs=['include'],
        library_dirs=['qdb'],
        libraries=['qdb_api'],
        extra_compile_args=extra_compile_args,
        extra_link_args=extra_link_args)])

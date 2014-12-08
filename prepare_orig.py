#!/usr/bin/env python
# (c)Bureau 14 SARL. All rights reserved.
# qdb is a trademark of Bureau 14 SARL

import os
import shutil

package_include_dir = os.path.join('include', 'qdb')
dirs_to_build = [package_include_dir, 'src', 'qdb']

''' parameters given by CMake '''
header_files = ["@QDB_API_C_HEADER@", "@QDB_API_CPP_HEADER@"]
library_dir = "@ACTUAL_LIBRARY_PATH@"
package_modules = ['@QDB_API_DLL@'] 

python_modules = ['@QDB_PY_IMPL@', '@QDB_PY_INTERFACE@']

for d in dirs_to_build:
    if not os.path.exists(d):
        os.makedirs(d)

# copy header
for f in header_files:
    shutil.copy(f, package_include_dir)
shutil.copy('@QDB_PY_WRAPPER@', 'src')

# copy windows lib required for linking
lib = os.path.join(library_dir, 'qdb_api.lib')
if os.path.exists(lib):
    shutil.copy(lib, 'qdb')

# filter out all empty fields
all_modules = filter(lambda x: len(x) > 0, package_modules + python_modules)

# copy package files
for mod in all_modules:
    shutil.copy(mod, 'qdb')

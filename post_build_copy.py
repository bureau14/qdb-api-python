#!/usr/bin/env python
# (c)Bureau 14 SARL. All rights reserved.
# qdb is a trademark of Bureau 14 SARL

import os
import shutil
import glob
import sys

if __name__ == '__main__':
    try:
        # should be one dir that start with lib

        lib_dir = glob.glob(os.path.join('build', 'lib*'))[0]
        path_to_file = os.path.join(lib_dir, 'qdb')
        lib_full_path = glob.glob(os.path.join(path_to_file, '_qdb.*'))[0]
        lib_file = os.path.split(lib_full_path)[1]

        shutil.copyfile(lib_full_path, os.path.join('qdb', lib_file))

        print "File copied successfully"

    except BaseException, e:
        print "Could not copy file: " + str(e)
        sys.exit(1)


    sys.exit(0)

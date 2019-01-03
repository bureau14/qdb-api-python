# Copyright (c) 2009-2019, quasardb SAS
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#    * Redistributions of source code must retain the above copyright
#      notice, this list of conditions and the following disclaimer.
#    * Redistributions in binary form must reproduce the above copyright
#      notice, this list of conditions and the following disclaimer in the
#      documentation and/or other materials provided with the distribution.
#    * Neither the name of quasardb nor the names of its contributors may
#      be used to endorse or promote products derived from this software
#      without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY QUASARDB AND CONTRIBUTORS ``AS IS'' AND ANY
# EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE REGENTS AND CONTRIBUTORS BE LIABLE FOR ANY
# DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
# (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
# ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
# SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#

from __future__ import print_function
from builtins import int

import os
import sys
import traceback
import datetime

import quasardb  # pylint: disable=C0413,E0401
import numpy as np

def main(quasardb_uri, blob_name):
    print("Connecting to: ", quasardb_uri)
    c = quasardb.Cluster(uri=quasardb_uri)

    # create a blob object
    b = c.blob(blob_name)

    # add data to the blob (which may, or may not exist)
    b.update('content')

    # print the content
    print("content:", b.get())

    # you can set expiry to blobs
    # by default there is none
    print("expiry =", b.get_expiry_time())

    # absolute expiry
    b.expires_at(datetime.datetime.now() + datetime.timedelta(minutes=2))
    print("expiry =", b.get_expiry_time())

    # relative expiry
    b.expires_from_now(datetime.timedelta(minutes=2))
    print("expiry =", b.get_expiry_time())

    b.remove()

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri blob_name")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()

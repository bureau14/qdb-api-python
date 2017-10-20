# Copyright (c) 2009-2017, quasardb SAS
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

import tempfile
import shutil
import os
import traceback
import sys
import subprocess
import time

from yahoo_finance import Share
from zipfile import ZipFile

# you don't need the following, it's just added so it can be run from the git repo
# without installing the quasardb library
for root, dirnames, filenames in os.walk(os.path.join('..', 'build')):
    for p in dirnames:
        if p.startswith('lib'):
            sys.path.append(os.path.join(root, p))

import quasardb  # pylint: disable=C0413,E0401

dji_tickers = frozenset(["mmm", "axp", "aapl", "ba", "cat", "cvx", "csco", "ko", "dis", "xom",
                         "ge",  "gs",  "hd",   "ibm","intc","jnj", "jpm",  "mcd","mrk", "msft",
                         "nke", "pfe", "pg",   "trv","utx", "unh", "vz",   "v",  "wmt", "dd"])

def trim_suffix(str, suffix):
    idx = str.find(suffix)
    return str if idx == -1 else str[:idx]

def clean_name(company_name):
    cleaned = company_name.lower()
    cleaned = trim_suffix(cleaned, ' group')
    cleaned = trim_suffix(cleaned, ' corp')
    cleaned = trim_suffix(cleaned, ' inc')
    cleaned = trim_suffix(cleaned, ' co.')
    cleaned = trim_suffix(cleaned, ' llc')
    cleaned = trim_suffix(cleaned, ' limited')
    cleaned = trim_suffix(cleaned, '.com')
    cleaned = cleaned.strip()
    cleaned = cleaned.replace(',', '').replace('.', '').replace('&', '').replace("'", '').replace('-', '_').replace(')', '').replace('(', '').replace(' ', '_')
    cleaned = cleaned.replace('__', '_')

    return cleaned

def browse_zips(directory):
    for (dirpath, dirnames, filenames) in os.walk(directory):
        for filename in filenames:
            if filename.endswith('.zip'):
                yield os.path.join(dirpath, filename)

def insert_via_binary(quasardb_uri, file, key_name):
    subprocess.check_call(["qdb_insert_csv_cpp", quasardb_uri, file, key_name])

def remove_entry(q, key_name):
    try:
        ts = q.ts(key_name)
        ts.remove()
    except Exception as e:
        pass

def tag_entry(q, key_name, ticker_name, ticker):

    tickers = [ticker.get_currency().lower(), ticker_name, ticker.get_stock_exchange().lower()]

    if ticker_name in dji_tickers:
        tickers.append("dji")

    print('Tagging ' + key_name + ' with ' + str(tickers))

    ts = q.ts(key_name)

    for t in tickers:
        ts.attach_tag(t)

def display_elapsed(start_time):
    print("...done in {} seconds".format(time.time() - start_time))

def get_ticker(ticker_name):
    while True:
        try:
            ticker = Share(ticker_name)
            return ticker
        except Exception as e:
            time.sleep(5)
            pass

def main(quasardb_uri, directory):

    q = quasardb.Cluster(quasardb_uri)

    work_dir = tempfile.mkdtemp(prefix='qdb')

    for zip_file in browse_zips(directory):
        with ZipFile(zip_file, 'r') as zf:
            for f in zf.namelist():
                ticker_name = os.path.splitext(f)[0].lower()
                try:
                    start_time = time.time()

                    ticker = get_ticker(ticker_name)

                    key_name = 'stocks.' + clean_name(ticker.get_name())

                    print('Importing ticker {} ({}) into {}...'.format(ticker_name, ticker.get_name(), key_name))
                    zf.extract(f, work_dir)

                    full_path = os.path.join(work_dir, f)

                    remove_entry(q, ticker_name)

                    insert_via_binary(quasardb_uri, full_path, key_name)

                    tag_entry(q, key_name, ticker_name, ticker)

                    os.remove(full_path)

                    display_elapsed(start_time)

                except Exception as ex:
                    print('Could not insert {}: {}'.format(ticker_name, str(ex)))

    shutil.rmtree(work_dir)

if __name__ == "__main__":

    try:
        if len(sys.argv) != 3:
            print("usage: ", sys.argv[0], " quasardb_uri directory")
            sys.exit(1)

        main(sys.argv[1], sys.argv[2])

    except Exception as ex:  # pylint: disable=W0703
        print("An error ocurred:", str(ex))
        traceback.print_exc()
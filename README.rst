quasardb Python API
===================

Installation
------------

Experimental installation using ``pip`` or ``easy_install``.

On Linux and FreeBSD:

::

        easy_install -i https://testpypi.python.org/pypi quasardb

On macOS and Windows:

::

        pip install -i https://testpypi.python.org/pypi quasardb

quasardb C API
~~~~~~~~~~~~~~

To build the Python API, you will need the C API. It can either be
installed on the machine (e.g. on unix in ``/usr/lib`` or ``/usr/local/lib``) or
you can unpack the C API archive in qdb.

Building the extension
~~~~~~~~~~~~~~~~~~~~~~

You will need `CMake <http://www.cmake.org/>`__,
`SWIG <http://www.swig.org/>`__ and the Python dist tools installed. You
can also download a pre-compiled package from our download site.

First, run cmake to create a project directory, for example:

::

        mkdir build
        cd build
        cmake -G "your generator" ..

Depending on the generator you chose, you will then either have to run
make or open the solution with your editor (e.g. Visual Studio).

For example on UNIX:

::

        mkdir build
        cd build
        cmake -G "Unix Makefiles" ..
        make

Usage
-----

Using *quasardb* starts with a Cluster:

.. code:: python

        import quasardb

        c = quasardb.Cluster('qdb://127.0.0.1:2836')

You can also establish a secure connection in case your cluster is set up for that:

.. code:: python
        c = quasardb.Cluster(uri='qdb://127.0.0.1:2836',
                             user_name='qdbuser',
                             user_private_key='/var/lib/qdb/user_private.key')

Now that we have a connection to the cluster, let's store some binary
data:

.. code:: python

        b = c.blob('bam')

        b.put('boom')
        v = b.get() # returns 'boom'

Want a queue? We have distributed queues.

.. code:: python

        q = c.deque('q2')

        q.push_back('boom')
        v = q.pop_front() # returns 'boom'

        q.push_front('bang')

quasardb comes out of the box with server-side atomic integers:

.. code:: python

        i = c.integer('some_int')

        i.put(3)  # i is equal to 3
        i.add(7)  # i is equal to 10
        i.add(-5) # is equal to 5

We also provide distributed hash sets:

.. code:: python

        hset = c.hset('the_set')

        hset.insert('boom')

        hset.contains('boom') # True

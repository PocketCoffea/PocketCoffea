Analysis  performance
=====================



CPU profiling
-------------


For profiling the CPU time of each function please select the *iterative* processor and then run
python as:

.. code-block:: bash
                
    python -m cProfile -o profiling/output.prof scripts/runner.py --cfg config.py --test -lf 10 -lc 10

Running on a few files should be enough to get stable results.

After getting the profiler output we analyze it with the `Snakeviz <https://jiffyclub.github.io/snakeviz/>`_
library

.. code-block:: bash
                
      snakeviz output.prof -s 

and open on a browser the link shown by the program.

Memory profiling
----------------

For memory profiling we use the `memray <https://github.com/bloomberg/memray>`_ library in single thread
(*iterative* processor.)

.. code-block:: bash

    python -m memray run -o profiling/memtest.bin scripts/runner.py --cfg config.py --test -lf 10 -lc 10

the output can be visualized in many ways. One of the most useful is the `flamegraph`: 

.. code-block:: bash

    memray flamegraph profiling/memtest.bin

then open the output .html file in you browser to explore the peak memory allocation. 

Alternatively the process can be monitored **live** during execution by doing:

.. code-block:: bash

     memray run --live scripts/runner.py --cfg config/config.py --test -lf 10 -lc 10


seasort
=======

This application implements a simple two-pass external sorting procedure of a
file that consists of fixed-size binary string records. It uses the
[seastar](http://github.com/scylladb/seastar) framework for performing IO
operations. It proceeds as follows:

 1. Sort fixed-size chunks of records in memory and write them to a temporary
    file.

 1. Merge the sorted chunks and write the result to the output file.

We assume that the memory:disk ratio is limited by 1:1000 so it doesn't seem
that we need to implement more than two passes as any modern SSD should easily
handle 1000 concurrent merge sources.

The application uses a single shard (i.e. it doesn't make sense to set the
`--smp` seastar option to a value greater than 1). Rationale:

 - It doesn't make sense to parallelize in-memory sorting, because the disk is
   many times slower than memory and so it wouldn't make any difference.

 - We don't really need CPU threads to make use of concurrency provided by
   modern SSD drives. Instead we can use Linux async-io, which is implemented
   under the hood by read/write file streams. All we need to do is set
   read-ahead and write-behind options.

How to build:

```
$ g++ seasort.cc $(pkg-config --libs --cflags --static $seastar_dir/build/release/seastar.pc) -o seasort
```

(The command above assumes that you pulled the
[seastar](https://github.com/scylladb/seastar) repository to `$seastar_dir`)

How to run:

```
$ ./seasort --smp 1 --in-file data --out-file data.out --chunk-size 128M
```

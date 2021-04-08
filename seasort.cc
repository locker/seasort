//
// This application implements a simple two-pass external sorting procedure
// of a file that consists of fixed-size binary string records. It proceeds
// as follows:
//
//  1. Sort fixed-size chunks of records in memory and write them to
//     a temporary file.
//  2. Merge the sorted chunks and write the result to the output file.
//
// By the problem description the memory:disk ratio is limited by 1:1000
// so it doesn't seem that we need to implement more than two passes as any
// modern SSD should easily handle 1000 concurrent merge sources.
//
// The application uses a single shard (i.e. it doesn't make sense to set
// the --smp option to a value greater than 1). Rationale:
//
//  - It doesn't make sense to parallelize in-memory sorting, because
//    the disk is many times slower than memory and so it wouldn't make
//    any difference.
//  - We don't really need CPU threads to make use of concurrency provided
//    by modern SSD drives. Instead we can use Linux async-io, which is
//    implemented under the hood by read/write file streams. All we need
//    to do is set read-ahead and write-behind options.
//
// How to build:
//
//  $ g++ seasort.cc $(pkg-config --libs --cflags --static $seastar_dir/build/release/seastar.pc) -o seasort
//
//  ( The command above assumes that you pulled the seastar repository to
//    $seastar_dir. See https://github.com/scylladb/seastar. )
//
// How to run:
//
//  $ ./seasort --smp 1 --in-file data --out-file data.out --chunk-size 128M
//

#include <cstdio>
#include <cstring>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include <boost/program_options.hpp>
#include <boost/iterator/counting_iterator.hpp>

#include <seastar/core/app-template.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/future-util.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/conversions.hh>

using namespace seastar;

// Size of a sorted record, in bytes.
size_t record_size;

// Size of a data chunk sorted in memory during the first pass of the external
// sorting algorithm, in bytes. Must be a multiple of the record size.
size_t chunk_size;

// Read and write options. Used during both sort and merge passes.
file_input_stream_options in_opts;
file_output_stream_options out_opts;

// A record to sort.
struct record {
    // Record data: a binary string of size equal to record_size.
    // The buffer may be shared with an input stream.
    temporary_buffer<char> buf;
    record() {} // EOF marker. Doesn't participate in comparisons.
    record(temporary_buffer<char>&& buf) : buf(std::move(buf)) {}
    bool operator<(const struct record& other) const {
        return memcmp(buf.get(), other.buf.get(), record_size) < 0;
    }
    bool operator>(const struct record& other) const {
        return other < *this;
    }
};

// Read data chunks from the input file, sort them in memory, and write them
// to the output file.
future<> sort_chunks(file in_file, file out_file) {
    // Rather than reading a whole chunk in-memory and then sorting it using
    // std::sort(), use an std::multiset to sort records as we go so as not
    // to block the event loop for too long.
    return do_with(std::multiset<record>(),
                   make_file_input_stream(in_file, in_opts),
                   make_file_output_stream(out_file, out_opts),
                   [] (auto& data, auto& in, auto& out) {
        // For each data chunk ...
        return repeat([&data, &in, &out] {
            // Read records from the chunk and store them in a sorted set.
            return repeat([&data, &in] {
                return in.read_exactly(record_size).then([&data] (auto buf) {
                    if (buf.empty())
                        return stop_iteration::yes;
                    data.emplace(buf.share());
                    if (data.size() >= chunk_size / record_size)
                        return stop_iteration::yes;
                    return stop_iteration::no;
                });
            // Upon reading the chunk, write the set to the output file.
            }).then([&data, &out] {
                return do_for_each(data.begin(), data.end(), [&out] (auto& rec) {
                    return out.write(rec.buf.get(), rec.buf.size());
                });
            // Finally, clear the set and repeat until the whole input file
            // has been processed.
            }).then([&data, &in] {
                data.clear();
                return in.eof() ? stop_iteration::yes : stop_iteration::no;
            });
        }).then([&in] {
            return in.close();
        }).then([&out] {
            return out.flush();
        });
    });
}

// Encapsulates a source for the merge procedure (the second pass).
// Used for reading records from a sorted chunk.
struct merge_source {
    record rec;             // current record (empty if eof)
    input_stream<char> in;  // input stream for fetching records

    merge_source(file f, uint64_t offset, uint64_t len) :
        in(make_file_input_stream(f, offset, len, in_opts)) {}

    future<> next() {
        return in.read_exactly(record_size).then([this] (auto buf) {
            if (!buf.empty()) {
                rec.buf = buf.share();
                return make_ready_future<>();
            } else {
                // EOF. Close the input stream.
                rec.buf = temporary_buffer<char>();
                return in.close();
            }
        });
    }

    bool eof() {
        return rec.buf.empty();
    }
};

// Chunk merger that performs the second pass of the external sorting algorithm.
struct merger {
    file in_file;   // file containing sorted data chunks produced by the first pass
    file out_file;  // file to write the sorted record set to

    // Array of merge sources.
    std::vector<merge_source> sources;

    merger(file in_file, file out_file) : in_file(in_file), out_file(out_file) {}

    // Helper function that creates merge sources and starts iteration.
    future<> start() {
        return in_file.size().then([this] (uint64_t size) {
            int chunk_count = (size + chunk_size - 1) / chunk_size;
            return do_for_each(boost::counting_iterator<int>(0),
                               boost::counting_iterator<int>(chunk_count),
                               [this] (int i) {
                uint64_t offset = static_cast<uint64_t>(i) * chunk_size;
                sources.emplace_back(in_file, offset, chunk_size);
                return sources.back().next();
            });
        });
    }

    // Helper function that merges sources until all of them have been exhausted.
    // Called after start().
    future<> run() {
        // Store indexes of the merge sources in a heap so that the source with
        // the smallest record is always at the top.
        auto cmp = [this] (int i, int j) {
            return sources[i].rec > sources[j].rec;
        };
        return do_with(std::priority_queue<int, std::vector<int>, decltype(cmp)>(cmp),
                       make_file_output_stream(out_file, out_opts),
                       [this] (auto& heap, auto& out) {
            // Push all sources to the heap.
            for (int i = 0; i < static_cast<int>(sources.size()); ++i) {
                if (!sources[i].eof())
                    heap.push(i);
            }
            // Until the heap is empty, write the record from the top source to
            // the output file, fetch the next record, and update the heap.
            return repeat([this, &heap, &out] {
                int i = heap.top();
                heap.pop();
                auto& src = sources[i];
                return out.write(src.rec.buf.get(), src.rec.buf.size()).then([&src] {
                    return src.next();
                }).then([&heap, &src, i] {
                    if (!src.eof())
                        heap.push(i);
                    return heap.empty() ? stop_iteration::yes : stop_iteration::no;
                });
            }).then([&out] {
                return out.flush();
            });
        });
    }

    future<> operator()() {
        return start().then([this] {
            return run();
        });
    }
};

// Merge sorted data chunks stored in the input file and write the result to the output file.
future<> merge_chunks(file in_file, file out_file) {
    return do_with(merger(in_file, out_file), [] (auto& merger) { return merger(); });
}

// Execute a simple two-pass (sort chunks, then merge them) external sorting procedure
// on the given input file. Write the result to the output file. Store sorted chunks to
// be merged in the given temporary file.
future<> ext_sort(file in_file, file out_file, file tmp_file) {
    return sort_chunks(in_file, tmp_file).then([tmp_file, out_file] {
        return merge_chunks(tmp_file, out_file);
    });
}

int main(int argc, char** argv) {
    app_template app;
    app.add_options()
        ("in-file", boost::program_options::value<std::string>()->required(),
         "input file name")
        ("out-file", boost::program_options::value<std::string>()->required(),
         "output file name")
        ("record-size", boost::program_options::value<std::string>()->default_value("4k"),
         "record size")
        ("chunk-size", boost::program_options::value<std::string>()->default_value("128M"),
         "size of data chunk sorted in memory")
        ("read-buffer-size", boost::program_options::value<std::string>()->default_value("32k"),
         "read buffer size")
        ("read-ahead", boost::program_options::value<unsigned>()->default_value(4),
         "maximum number of extra read-ahead operations per input stream")
        ("write-buffer-size", boost::program_options::value<std::string>()->default_value("256k"),
         "write buffer size")
        ("write-behind", boost::program_options::value<unsigned>()->default_value(8),
         "maximum number of buffers to write in parallel")
        ;
    app.run(argc, argv, [&app] {
        auto& args = app.configuration();
        auto in_file_name = args["in-file"].as<std::string>();
        auto out_file_name = args["out-file"].as<std::string>();
        record_size = parse_memory_size(args["record-size"].as<std::string>());
        chunk_size = parse_memory_size(args["chunk-size"].as<std::string>());
        in_opts.buffer_size = parse_memory_size(args["read-buffer-size"].as<std::string>());
        in_opts.read_ahead = args["read-ahead"].as<unsigned>();
        out_opts.buffer_size = parse_memory_size(args["write-buffer-size"].as<std::string>());
        out_opts.write_behind = args["write-behind"].as<unsigned>();

        // Make sure the chunk size is a multiple of the record size.
        chunk_size = (chunk_size + record_size - 1) / record_size * record_size;

        // Path to the file used for storing sorted chunks to be merged.
        auto tmp_file_name = out_file_name + ".tmp";

        // Open files and proceed to the external sorting procedure.
        return when_all_succeed(open_file_dma(in_file_name, open_flags::ro),
                                open_file_dma(out_file_name, open_flags::wo |
                                              open_flags::create | open_flags::truncate),
                                open_file_dma(tmp_file_name, open_flags::rw |
                                              open_flags::create | open_flags::truncate)
                               ).then([] (auto in_file, auto out_file, auto tmp_file) {
            return ext_sort(in_file, out_file, tmp_file);
        }).finally([tmp_file_name] {
            // Remove the temporary file once we are done.
            std::remove(tmp_file_name.c_str());
        });
    });
}

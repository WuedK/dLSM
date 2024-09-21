// added by Arman -> 20 September 2024

#ifndef COMPUTE_LOAD_INFO_H_
#define COMPUTE_LOAD_INFO_H_

#include "db/memtable.h"

#include "util/testlog.h"

#include <stddef.h>
#include <atomic>

namespace TimberSaw {

class Cmp_Side_Load_Info {
public:
    explicit Cmp_Side_Load_Info(uint_8 id) : num_laccess(0), num_rreads(0), num_flushes(0), shard_id(id) {}

    // void increment_lreads(size_t num = 1) {
    //     num_lreads.fetch_add(num);
    // }

    void increment_local_access(size_t num = 1) {
        // num_laccess.fetch_add(num);
        LOGFC(COLOR_CYAN, stdout, "shard_id %hhu: num local access: %lu\n", shard_id, num_laccess.fetch_add(num) + num);
    }

    void increment_rreads(size_t num = 1) {
        // num_rreads.fetch_add(num);
        LOGFC(COLOR_CYAN, stdout, "shard_id %hhu: num remote reads: %lu\n", shard_id,num_rreads.fetch_add(num) + num);
    }

    // void increment_lwrites(size_t num = 1) {
    //     num_writes.fetch_add(num);
    // }

    void increment_flushes(size_t num = 1) {
        // num_flushes.fetch_add(num);
        LOGFC(COLOR_CYAN, stdout, "shard_id %hhu: num flushes: %lu\n", shard_id, num_flushes.fetch_add(num) + num);
    }

    size_t compute_load() {
        // return num_laccess.exchange(0) * local_access_time 
        //      + num_rreads.exchange(0) * remote_read_time 
        //     //  + num_writes.exchange(0) * local_write_time 
        //      + num_flushes.exchange(0) * flush_time; 

        size_t load = num_laccess.exchange(0) * local_access_time 
             + num_rreads.exchange(0) * remote_read_time 
            //  + num_writes.exchange(0) * local_write_time 
             + num_flushes.exchange(0) * flush_time; 

        LOGFC(COLOR_CYAN, stdout, "shard_id %hhu: load incremented by: %lu\n", shard_id, load);

        return load;
    }

private:

    // static constexpr size_t local_read_time = 1;
    static constexpr size_t remote_read_time = 10;
    // static constexpr size_t local_write_time = 1;
    static constexpr size_t flush_time = 100;
    static constexpr size_t local_access_time = 1;
    // static constexpr size_t mem_table_cap_mb = 64;
    // static constexpr size_t key_size = 64;
    // static constexpr size_t value_size = 64;


    // std::atomic<size_t> num_lreads;
    // std::atomic<size_t> num_writes;
    std::atomic<size_t> num_laccess;
    std::atomic<size_t> num_rreads;
    std::atomic<size_t> num_flushes;
    const uint8_t shard_id;
};

}

#endif
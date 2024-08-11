//
// Created by arman on 7/23/24.
//

#ifndef TimberSaw_LOAD_BALANCER_H
#define TimberSaw_LOAD_BALANCER_H

#include <stdio.h>
#include <stdint.h>
// #include <map>
#include <set>
#include <vector>

namespace TimberSaw {

// class Timed_Data {
//     static constexpr size_t time_level = 5;
//     static constexpr double ratio[time_level] = {0.5, 0.2, 0.15, 0.1, 0.05};

//     size_t data[time_level];

//     void pass() {
//         for (int i = (time_level - 1); i >= 1; ++i) {
//             data[i] = data[i-1];
//         }
//         data[0] = 0;
//     }

//     void increment(size_t amount) {
//         data[0] += amount;
//     }

//     void update(size_t new_val) {
//         data[0] = new_val;
//     }

//     size_t overal_value() {
//         size_t res = 0;
//         for (int i = 0; i < time_level; ++i) {
//             res += (size_t)(std::floor(ratio[i] * data[i]));
//         }
//         return res;
//     }

//     size_t& operator[](size_t idx) {
//         if (idx < 0 || idx >= time_level)
//             throw std::out_of_range();
//         return data[idx];
//     }
// };

struct Load_Info {

    static constexpr size_t local_read_time = 0;
    static constexpr size_t remote_read_time = 0;
    static constexpr size_t local_write_time = 0;
    static constexpr size_t flush_time = 0;
    static constexpr size_t mem_table_cap_mb = 64;

    size_t num_reads = 0;
    size_t num_writes = 0;
    size_t num_remote_reads = 0;
    size_t num_flushes = 0;
    // size_t num_files;
    // size_t size_MB;

    // size_t num_cached_files;
    // size_t cached_size_MB;

    size_t last_load = 0;

    void compute_load_and_pass() {
        last_load = (last_load / 2)
                + num_reads * local_read_time 
                + num_remote_reads * remote_read_time 
                + num_writes * local_write_time
                + num_flushes * flush_time;
        num_reads = 0;
        num_remote_reads = 0;
        num_writes = 0;
        num_flushes = 0;
    }
};

struct Shard_Info {
    uint8_t owner;
    size_t id;
    Load_Info load;

    // std::string from, to;
};

struct Compute_Node_Info {
    std::set<size_t> shards;
    size_t overal_load;
    uint8_t node_id;
};

class Load_Balancer {
public:
    Load_Balancer(size_t num_compute, size_t num_shards_per_compute);
    ~Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    void new_bindings(); // returns a new ownership map -> will replace compute_node_info when ready
    void set_up_new_plan(); // gets a new optimal plan and executes a protocol to make sure things are running. -> run by load_balancer or main thread?
    
    // functions for updating load info per shard and node

private:
    std::vector<Compute_Node_Info> compute_node_info;
    std::vector<Shard_Info> shards; // contains shard info as well as which shard is owned by who Compute_Node_Info->shards are pointers to this vector.

    const unsigned int reschdule_period_seconds = 30;
};

}

#endif
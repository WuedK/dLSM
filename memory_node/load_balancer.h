//
// Created by arman on 7/23/24.
//

#ifndef TimberSaw_LOAD_BALANCER_H
#define TimberSaw_LOAD_BALANCER_H

#include <stdint.h>
#include <map>
#include <set>
#include <vector>

namespace TimberSaw {

template <typename T>
struct Timed_Data {
    std::vector<T> data;

    void pass(); // passes time

};

struct Load_Info {

    static constexpr size_t remote_read_time = 0;
    static constexpr size_t flush_time = 0;

    size_t local_read_time = 0; // ?
    size_t local_write_time = 0; // ?

    Timed_Data<size_t> num_reads, num_writes;
    size_t num_files;
    size_t size_MB;

    size_t num_cached_files;
    size_t cached_size_MB;

    size_t last_load = 0;

    size_t compute_load() {
        // computes the current load and sets last_load to it -> then retuns the new load
    }
};

struct Shard_Info {
    size_t id;
    Load_Info load;
    uint8_t owner;

    // Range()?
};

struct Compute_Node_Info {
    std::set<Shard_Info*> shards;
    Load_Info overal_load;
    uint8_t node_id;
};

class Load_Balancer {
public:
    Load_Balancer();
    ~Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    std::map<uint8_t, Compute_Node_Info>* new_bindings(); // returns a new ownership map -> will replace compute_node_info when ready
    void set_up_new_plan(); // gets a new optimal plan and executes a protocol to make sure things are running. -> run by load_balancer or main thread?
    
    // functions for updating load info per shard and node

private:
    std::map<uint8_t, Compute_Node_Info>* compute_node_info;
    std::vector<Shard_Info> shards; // contains shard info as well as which shard is owned by who Compute_Node_Info->shards are pointers to this vector.
};

}

#endif
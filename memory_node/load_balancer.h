//
// Created by arman on 7/23/24.
//

#ifndef TimberSaw_LOAD_BALANCER_H
#define TimberSaw_LOAD_BALANCER_H

#include "load_info_container.h"


namespace TimberSaw {


class Load_Balancer {
public:
    Load_Balancer(uint8_t num_compute, size_t num_shards_per_compute);
    ~Load_Balancer();
    void start(); // runs a thread which periodically does load balancing and then sleeps
    void new_bindings(); // returns a new ownership map -> will replace compute_node_info when ready
    void set_up_new_plan(); // gets a new optimal plan and executes a protocol to make sure things are running. -> run by load_balancer or main thread?
    
    // functions for updating load info per shard and node

private:
    inline static int check_load(size_t load, size_t mean_load) {
        if (load > mean_load && load - mean_load > load_imbalance_threshold_half) {
            return 2;
        }
        else if (load > mean_load && load - mean_load <= load_imbalance_threshold_half) {
            return 1;
        }
        else if (load == mean_load) {
            return 0;
        }
        else if (load < mean_load && mean_load - load <= load_imbalance_threshold_half) {
            return -1;
        }
        else {
            return -2;
        }
    }

    // std::vector<Compute_Node_Info> compute_node_info;
    // std::vector<Shard_Info> shards; // contains shard info as well as which shard is owned by who Compute_Node_Info->shards are pointers to this vector.
    Load_Info_Container container;
    static constexpr unsigned int rebalance_period_seconds = 30;
    static constexpr size_t load_imbalance_threshold = 100;
    static constexpr size_t load_imbalance_threshold_half = load_imbalance_threshold / 2;
    static constexpr size_t low_load_thresh = 0;
};

}

#endif
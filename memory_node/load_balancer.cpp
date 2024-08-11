//
// Created by arman on 8/11/24.
//

#include "load_balancer.h"

#include <unistd.h>

namespace TimberSaw {

    void compute_load_for_node_and_pass(Compute_Node_Info& node, std::vector<Shard_Info>& shards) {
        node.overal_load = 0;
        for (auto shard_idx : node.shards) {
            shards[shard_idx].load.compute_load_and_pass();
            node.overal_load += shards[shard_idx].load.last_load;
        }
    }

    Load_Balancer::Load_Balancer(size_t num_compute, size_t num_shards_per_compute) 
        : compute_node_info(num_compute), shards(num_shards_per_compute) {
            
        for (size_t i = 0; i < num_compute; ++i) {
            compute_node_info[i].node_id = i;
            for(int j = 0; j < num_shards_per_compute; ++j)
                compute_node_info[i].shards.insert(i * num_shards_per_compute + j);
        }

        for(int i = 0; i < num_shards_per_compute * num_compute; ++i) {
            shards[i].id = i;
            shards[i].owner = i / num_shards_per_compute;
        }
    }

    Load_Balancer::~Load_Balancer() {

    }

    void Load_Balancer::start() {
        std::set<size_t> seen_nodes;

        while (true) {
            sleep(reschdule_period_seconds);
            Compute_Node_Info* min_node = nullptr;
            Compute_Node_Info* max_node = nullptr;
            for (Compute_Node_Info& node : compute_node_info) {
                compute_load_for_node_and_pass(node, shards);
                if (min_node == nullptr || min_node->overal_load > node.overal_load) {
                    min_node = &node;
                }
                if (max_node == nullptr || max_node->overal_load < node.overal_load) {
                    max_node = &node;
                }

            }
        }
    }

    void Load_Balancer::new_bindings() {

    }

    void Load_Balancer::set_up_new_plan() {

    }
    
    // functions for updating load info per shard and node


}
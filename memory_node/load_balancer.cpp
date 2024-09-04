//
// Created by arman on 8/11/24.
//

#include "load_balancer.h"

#include <unistd.h>
#include <algorithm>
#include <vector>

namespace TimberSaw {

    // void compute_load_for_node_and_pass(Compute_Node_Info& node, std::vector<Shard_Info>& shards) {
    //     node.overal_load = 0;
    //     for (auto shard_idx : node.shards) {
    //         shards[shard_idx].load.compute_load_and_pass();
    //         node.overal_load += shards[shard_idx].load.last_load;
    //     }
    // }

    // Load_Balancer::Load_Balancer(size_t num_compute, size_t num_shards_per_compute) 
    //     : compute_node_info(num_compute), shards(num_shards_per_compute) {
            
    //     for (size_t i = 0; i < num_compute; ++i) {
    //         compute_node_info[i].node_id = i;
    //         for(int j = 0; j < num_shards_per_compute; ++j)
    //             compute_node_info[i].shards.insert(i * num_shards_per_compute + j);
    //     }

    //     for(int i = 0; i < num_shards_per_compute * num_compute; ++i) {
    //         shards[i].id = i;
    //         shards[i].owner = i / num_shards_per_compute;
    //     }
    // }

    // Load_Balancer::~Load_Balancer() {

    // }

    // void Load_Balancer::start() {
    //     std::set<size_t> seen_nodes;

    //     while (true) {
    //         sleep(reschdule_period_seconds);

    //         Compute_Node_Info* min_node = nullptr;
    //         Compute_Node_Info* max_node = nullptr;
    //         for (Compute_Node_Info& node : compute_node_info) {
    //             compute_load_for_node_and_pass(node, shards);
    //             if (min_node == nullptr || min_node->overal_load > node.overal_load) {
    //                 min_node = &node;
    //             }
    //             if (max_node == nullptr || max_node->overal_load < node.overal_load) {
    //                 max_node = &node;
    //             }
    //         }
    //     }
    // }

    // void Load_Balancer::new_bindings() {

    // }

    // void Load_Balancer::set_up_new_plan() {

    // }

    Load_Balancer::Load_Balancer(uint8_t num_compute, size_t num_shards_per_compute) 
        : container(num_compute, num_shards_per_compute) {
            
        // for (size_t i = 0; i < num_compute; ++i) {
        //     compute_node_info[i].node_id = i;
        //     for(int j = 0; j < num_shards_per_compute; ++j)
        //         compute_node_info[i].shards.insert(i * num_shards_per_compute + j);
        // }

        // for(int i = 0; i < num_shards_per_compute * num_compute; ++i) {
        //     shards[i].id = i;
        //     shards[i].owner = i / num_shards_per_compute;
        // }
    }

    Load_Balancer::~Load_Balancer() {

    }

    void Load_Balancer::start() {
        while (true) {
            sleep(rebalance_period_seconds);

            size_t min_load;
            size_t max_load;
            size_t mean_load = 0;

            container.compute_load_and_pass(min_load, max_load, mean_load);
            if (max_load - min_load <= load_imbalance_threshold) {
                continue;
            }

            int min_stat = check_load(container.min_node().load(), mean_load);
            int max_stat = check_load(container.max_node().load(), mean_load);
            while ((max_stat > 1 || min_stat < -1) && max_stat > -2 && min_stat < 2) { // loop on nodes
                Compute_Node_Info& max_node = container.max_node();
                Shard_Iterator& itr = max_node.ordered_iterator();

                while (itr.is_valid()) { // loop on shards
                    if (container.is_insignificant(*(itr.shard()))) {
                        break;
                    }

                    int hload_stat = check_load(max_node.load() - itr.shard()->load(), mean_load);
                    int lload_stat = check_load(container.min_node().load() + itr.shard()->load(), mean_load);
                    if (hload_stat < -1 || lload_stat > 1) {
                        // it may be possible that continuing with this would result in better balance
                        // while both nodes still remain out of prefered range but keep in mind that
                        // ownership transfer increases the load of a shard. Therefore, the oposit may happen
                        // as well and transfer is not worth it here.
                        // In these cases, it is better to increase num shards. (which we cannot do in current design)
                        ++itr;
                        continue;
                    }
                    
                    container.change_owner_from_max_to_min(*(itr.shard()));
                    ++itr;
                    if (hload_stat < 2 && lload_stat > -2) {
                        break;
                    }
                    
                }
                container.update_max_load();

                if (&max_node == &container.max_node()) {
                    container.ignore_max();
                }

                min_stat = check_load(container.min_node().load(), mean_load);
                max_stat = check_load(container.max_node().load(), mean_load);
            }

            set_up_new_plan();
        }
    }

    void Load_Balancer::new_bindings() {

    }

    void Load_Balancer::set_up_new_plan() {
        container.apply();
    }
    
    // functions for updating load info per shard and node


}
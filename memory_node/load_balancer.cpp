//
// Created by arman on 8/11/24.
//

#include "load_balancer.h"
#include "../util/testlog.h"

#include <unistd.h>
#include <algorithm>
#include <vector>
#include <assert.h>

namespace TimberSaw {

    Load_Balancer::Load_Balancer(size_t num_compute, size_t num_shards_per_compute) 
        : container(num_compute, num_shards_per_compute), started(false) {}

    Load_Balancer::~Load_Balancer() {}

    void Load_Balancer::start() {
        started.store(true);

        while (started.load()) {
            sleep(rebalance_period_seconds);

            size_t min_load;
            size_t max_load;
            size_t mean_load = 0;

            container.compute_load_and_pass(min_load, max_load, mean_load);
            // std::cout << "load computed!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n";

            // continue;

            if (max_load - min_load <= load_imbalance_threshold) { // use a statistic of shards(like max shard or mean shard as threshold)
                continue;
            }

            int min_stat = check_load(container.min_node().load(), mean_load);
            int max_stat = check_load(container.max_node().load(), mean_load);
            // TODO add something that if we have outlier do some shard, we recompute mean for the other nodes and try to balance those
            while ((max_stat > 1 || min_stat < -1) && max_stat > -2 && min_stat < 2) { // loop on nodes
                Compute_Node_Info& max_node = container.max_node();
                // std::cout << "before sort:\n";
                Shard_Iterator& itr = max_node.ordered_iterator();
                // std::cout << "after sort:\n";

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
                    
                    assert(&container.max_node() == &max_node);
                    container.change_owner_from_max_to_min(itr.index());
                    ++itr;
                    if (hload_stat < 2 && lload_stat > -2) {
                        break;
                    }
                    
                }
                container.update_max_load();

                if (&max_node == &container.max_node()) {
                    container.ignore_max(mean_load);
                }

                min_stat = check_load(container.min_node().load(), mean_load);
                max_stat = check_load(container.max_node().load(), mean_load);
            }

            set_up_new_plan();
        }
    }

    void Load_Balancer::shut_down() {
        started.store(false);
    }

    void Load_Balancer::set_up_new_plan() {
        char buffer_2[10000] = "";
        auto updates = container.apply();
        sprintf(buffer_2 + strlen(buffer_2), "* new changes: \n");
        // std::cout << "* new changes: \n";
        for (auto update : updates) {
            sprintf(buffer_2 + strlen(buffer_2), "Shard %lu from node %lu to node %lu\n", update.shard, update.from, update.to);
            // std:: cout << "Shard " << update.shard << " from node " << update.from << " to node " << update.to << "\n";
        }

        LOGF(stdout, "%s", buffer_2);

    }

    // void Load_Balancer::rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
    //     container.rewrite_load_info(shard, num_reads, num_writes, num_remote_reads, num_flushes);
    // }

    // void Load_Balancer::increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
    //     container.increment_load_info(shard, num_reads, num_writes, num_remote_reads, num_flushes);
    // }
    
    void Load_Balancer::increment_load_info(size_t shard, size_t added_load) {
        container.increment_load_info(shard, added_load);
    }

    // functions for updating load info per shard and node


}
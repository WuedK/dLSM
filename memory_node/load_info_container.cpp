#include "load_info_container.h"

namespace TimberSaw {
    Load_Info_Container::Load_Info_Container(uint8_t num_compute, size_t num_total_shards) {

    }

    Load_Info_Container::~Load_Info_Container() {

    }

    void Load_Info_Container::rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
        Shard_Info& _shard = shard_id(shard);
        _shard._load.num_reads = num_reads;
        _shard._load.num_writes = num_writes;
        _shard._load.num_remote_reads = num_remote_reads;
        _shard._load.num_flushes = num_flushes;
    }

    void Load_Info_Container::increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
        Shard_Info& _shard = shard_id(shard);
        _shard._load.num_reads += num_reads;
        _shard._load.num_writes += num_writes;
        _shard._load.num_remote_reads += num_remote_reads;
        _shard._load.num_flushes += num_flushes;
    }

    void Load_Info_Container::compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load) {
        ordered_nodes.clear();
        max_load_change = 0;
        cnodes[0].compute_load_and_pass();
        mean_load = cnodes[0]._overal_load;
        min_load = cnodes[0]._overal_load;
        max_load = cnodes[0]._overal_load;
        ordered_nodes.insert({cnodes[0]._overal_load, 0});
        for (uint8_t i = 1; i < cnodes.size(); ++i) {
            cnodes[i].compute_load_and_pass();
            mean_load += cnodes[i]._overal_load;
            if (min_load > cnodes[i]._overal_load) {
                min_load = cnodes[i]._overal_load;
            }
            if (max_load < cnodes[i]._overal_load) {
                max_load = cnodes[i]._overal_load;
            }
            ordered_nodes.insert({cnodes[i]._overal_load, i});
        }
        mean_load /= cnodes.size();
    }

    void Load_Info_Container::update_max_load() {
        if (max_load_change == 0)
            return;

        Compute_Node_Info& _max_node = max_node();
        assert(_max_node._overal_load > max_load_change);
        ordered_nodes.erase(std::prev(ordered_nodes.end()));
        ordered_nodes.insert({_max_node._overal_load - max_load_change, _max_node._id});
        max_load_change = 0;
    }

    void Load_Info_Container::change_owner_from_max_to_min(Shard_Info& shard) {
        Compute_Node_Info& from = max_node();
        Compute_Node_Info& to = min_node();
        assert(shard._owner == from._id);
        updates.push_back({from, to, shard});
        ordered_nodes.erase(ordered_nodes.begin());
        ordered_nodes.insert({to._overal_load + shard._load.last_load, to._id});
        max_load_change += shard._load.last_load;
    }

    void Load_Info_Container::apply() {

    }
}
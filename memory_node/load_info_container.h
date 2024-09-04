#ifndef LOAD_INFO_CONTAINER_H_
#define LOAD_INFO_CONTAINER_H_

#include <stdio.h>
#include <stdint.h>
#include <queue>
#include <map>
#include <assert.h>
#include <algorithm>
#include <stddef.h>

namespace TimberSaw {

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

class Shard_Info {
public:
    inline size_t load() const {
        return _load.last_load;
    }

    inline uint8_t owner() const {
        return _owner;
    }

    inline size_t id() const {
        return _id;
    }

    inline size_t index() const {
        return _index;
    }

    friend class Load_Info_Container;
    friend class Compute_Node_Info;
    
private:
    uint8_t _owner;
    size_t _id;
    size_t _index;
    Load_Info _load;

};

class Shard_Info_Cmp {
public:
    inline bool operator()(const Shard_Info& a, const Shard_Info& b) const {
        return a.load() < b.load();
    }
};

class Shard_Iterator {
public:
    Shard_Iterator(const Shard_Iterator&) = delete;
    Shard_Iterator& operator=(const Shard_Iterator&) = delete;

    inline bool is_valid() {
        return valid;
    }

    inline Shard_Iterator& operator++() {
        if (!valid || _shard_idx == 0 || _shard_idx >= _owner.num_shards()) {
            valid = false;
        }
        else {
            --_shard_idx;
        }
        return *this;
    }

    inline Shard_Info* shard() {
        return (valid ? &_owner[_shard_idx] : nullptr);
    }

    friend class Compute_Node_Info;
private:
    explicit Shard_Iterator(Compute_Node_Info* owner) : _owner(*owner) {
        reset();
    }
    // explicit Shard_Iterator(Compute_Node_Info& owner) : _owner(owner), _shard_idx(owner.num_shards() - 1) {}

    void reset() {
        _shard_idx = _owner.num_shards() - 1;
        valid = true;
    }

private:
    size_t _shard_idx;
    Compute_Node_Info& _owner;
    bool valid;
};

class Compute_Node_Info {
public:
    Compute_Node_Info() : itr(this) {}

    inline size_t load() const {
        return _overal_load;
    }

    inline size_t num_shards() const {
        return _size;
    }

    inline uint8_t id() const {
        return _id;
    }


    inline Shard_Info& operator[](size_t shard_idx) {
        return _shards[shard_idx];
    }

    inline Shard_Iterator& ordered_iterator() {
        return itr;
    }

    friend class Load_Info_Container;

private:
    inline void compute_load_and_pass() {
        _overal_load = 0;
        itr.reset();
        for (size_t i = 0; i < _shards.size(); ++i) {
            _shards[i]._load.compute_load_and_pass();
            _overal_load += _shards[i]._load.last_load;
        }
        std::sort(_shards.begin(), _shards.end(), Shard_Info_Cmp{});
    }

private:
    size_t _overal_load;
    uint8_t _id;
    size_t _size;
    std::vector<Shard_Info> _shards;
    Shard_Iterator itr;
    bool _has_itr = false;

};

struct Owner_Ship_Transfer {
    Compute_Node_Info& from;
    Compute_Node_Info& to;
    Shard_Info& shard;
};


class Load_Info_Container {
public:
    Load_Info_Container(uint8_t num_compute, size_t num_total_shards);
    ~Load_Info_Container();

    void rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load);
    void update_max_load();
    void change_owner_from_max_to_min(Shard_Info& shard);
    void apply();

    inline bool is_insignificant(Shard_Info& shard) {
        return shard._load.last_load == 0; // TODO better approaches?
    }

    inline void ignore_max() {
        ordered_nodes.erase(std::prev(ordered_nodes.end()));
        max_load_change = 0;
    }
    
    inline Compute_Node_Info& max_node() {
        return cnodes[ordered_nodes.rbegin()->second];
    }

    inline Compute_Node_Info& min_node() {
        return cnodes[ordered_nodes.begin()->second];
    }

    inline Shard_Info& shard_id(size_t shard_id) {
        return cnodes[shard_id_to_idx[shard_id].first][shard_id_to_idx[shard_id].second];
    }

    inline Compute_Node_Info& operator[](uint8_t compute_id) {
        return cnodes[compute_id];
    }


    inline size_t num_shards() {
        return _num_shards;
    }

    inline uint8_t num_compute() {
        return cnodes.size();
    }

private:
    size_t _num_shards;
    std::vector<Compute_Node_Info> cnodes;
    // Compute_Node_Info* cnodes;
    bool compute_sorted;
    std::vector<std::pair<uint8_t, size_t>> shard_id_to_idx;
    std::vector<Owner_Ship_Transfer> updates;
    std::multimap<size_t, uint8_t> ordered_nodes;
    size_t max_load_change = 0;
};

}

#endif
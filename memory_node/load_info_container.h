#ifndef LOAD_INFO_CONTAINER_H_
#define LOAD_INFO_CONTAINER_H_

#include <stdio.h>
#include <stdint.h>
#include <vector>
#include <map>
#include <stddef.h>
#include <mutex>
#include <shared_mutex>
#include <atomic>

#include <iostream>
#include <stdio.h>
#include <assert.h>
#include <cstring>

//TODO add cuncurrency -> we may need to update load info at the time of sorting and stuff

namespace TimberSaw {

struct Load_Info {

    static constexpr size_t local_read_time = 1;
    static constexpr size_t remote_read_time = 10;
    static constexpr size_t local_write_time = 1;
    static constexpr size_t flush_time = 100;
    static constexpr size_t mem_table_cap_mb = 64;

    std::atomic<size_t> num_reads{0};
    std::atomic<size_t> num_writes{0};
    std::atomic<size_t> num_remote_reads{0};
    std::atomic<size_t> num_flushes{0};

    std::atomic<size_t> temp_num_reads{0};
    std::atomic<size_t> temp_num_writes{0};
    std::atomic<size_t> temp_num_remote_reads{0};
    std::atomic<size_t> temp_num_flushes{0};

    // std::atomic<std::size_t> ip_num_reads(0);
    // std::atomic<size_t> ip_num_writes;
    // std::atomic<size_t> ip_num_remote_reads;
    // std::atomic<size_t> ip_num_flushes;

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

    void print(char* buffer) const {
        sprintf(buffer + strlen(buffer), "last_load: %lu, num_reads: %lu, num_writes: %lu, num_remote_reads: %lu, num_flushes: %lu\n"
            , last_load, num_reads.load(), num_writes.load(), num_remote_reads.load(), num_flushes.load());
    }
};

class Shard_Info {
public:
    inline size_t load() const {
        return _load.last_load;
    }

    inline size_t owner() const {
        return _owner;
    }

    inline size_t id() const {
        return _id;
    }

    // inline size_t index() const {
    //     return _index;
    // }

    inline void print(char* buffer) const {
        // printf("shard with id %lu is owned by cnode[%lu] in index %lu. The load of this shard is: ", _id, _owner, _index);
        sprintf(buffer + strlen(buffer), "shard with id %lu is owned by cnode[%lu]. The load of this shard is: ", _id, _owner);
        _load.print(buffer);
    }

    friend class Load_Info_Container;
    friend class Compute_Node_Info;
    
private:
    size_t _owner;
    size_t _id;
    // size_t _index;
    Load_Info _load;

};

class Compute_Node_Info;

class Shard_Iterator {
public:
    Shard_Iterator(const Shard_Iterator&) = delete;
    Shard_Iterator& operator=(const Shard_Iterator&) = delete;

    inline bool is_valid() {
        return valid;
    }

    Shard_Iterator& operator++();

    Shard_Info* shard();

    inline size_t index() {
        return _shard_idx;
    }

    friend class Compute_Node_Info;
    friend class Load_Info_Container;
private:
    explicit Shard_Iterator(Compute_Node_Info& owner);
    // explicit Shard_Iterator(Compute_Node_Info& owner) : _owner(owner) {
    //     reset();
    // }
    // explicit Shard_Iterator(Compute_Node_Info& owner) : _owner(owner), _shard_idx(owner.num_shards() - 1) {}

    void reset();

private:
    size_t _shard_idx;
    Compute_Node_Info& _owner;
    bool valid;
};

class Compute_Node_Info {
public:
    Compute_Node_Info() : _overal_load(0), itr(*this) {}
    // Compute_Node_Info(size_t num_shards) : _overal_load(0), _shards(num_shards), itr(*this) {}

    inline size_t load() const {
        return _overal_load;
    }

    inline size_t num_shards() const {
        return _shards.size();
    }

    inline size_t id() const {
        return _id;
    }


    inline Shard_Info& operator[](size_t shard_idx) {
        return *(_shards[shard_idx]);
    }

    inline Shard_Iterator& ordered_iterator() {
        sort_shards_if_needed();
        // std::cout << "just after sort\n";
        return itr;
    }


    inline void print(char* buffer) const {
        sprintf(buffer + strlen(buffer), "cnode with id %lu has overall load of %lu and owns %lu shards:\n\n", _id, _overal_load, _shards.size());
        // for (auto s : _shards) {
        //     std::cout << s->_id << ", ";
        // }
        // printf("\n");
    }

    friend class Load_Info_Container;

private:
    class Shard_Info_Pointer_Cmp {
    public:
        inline bool operator()(const Shard_Info* a, const Shard_Info* b) const {
            assert(a && b);
            return a->load() < b->load();
        }
    };

    // void shard_mergeSort(bool change_idx = true);
    // void shard_insertionSort(Shard_Info** arr, size_t N, Shard_Info_Pointer_Cmp cmp, bool first);
    // void shard_merge(size_t lidx, size_t midx, size_t ridx, Shard_Info_Pointer_Cmp cmp, bool first);
    // void set_shard_index();

    void sort_shards_if_needed();

    inline void compute_load_and_pass() {
        _overal_load = 0;
        is_sorted = false;
        itr.reset();
        for (size_t i = 0; i < _shards.size(); ++i) {
            _shards[i]->_load.compute_load_and_pass();
            _overal_load += _shards[i]->_load.last_load;
        }
    }

private:
    size_t _overal_load;
    size_t _id;
    std::vector<Shard_Info*> _shards;
    Shard_Iterator itr;
    bool is_sorted = false;
};

struct Owner_Ship_Transfer {
    size_t from;
    size_t to;
    size_t shard;
};

class Load_Info_Container {
public:
    Load_Info_Container(size_t num_compute, size_t num_total_shards);
    ~Load_Info_Container();

    // void rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes);
    void compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load);
    void update_max_load();
    void change_owner_from_max_to_min(size_t shard_idx);
    std::vector<Owner_Ship_Transfer> apply();

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
        return shards[shard_id];
    }

    inline Compute_Node_Info& operator[](size_t compute_id) {
        return cnodes[compute_id];
    }


    inline size_t num_shards() {
        return shards.size();
    }

    inline size_t num_compute() {
        return cnodes.size();
    }

private:
    void flush_load_changes();

private:
    std::vector<Compute_Node_Info> cnodes;
    std::vector<Shard_Info> shards;
    std::vector<Owner_Ship_Transfer> updates;
    std::multimap<size_t, size_t> ordered_nodes;
    size_t max_load_change = 0;
    std::shared_mutex mu;
};

}

#endif
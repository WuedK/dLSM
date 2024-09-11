#include "load_info_container.h"

#include <assert.h>
#include <algorithm>
#include <cstring>

namespace TimberSaw {

    Shard_Iterator::Shard_Iterator(Compute_Node_Info& owner) : _owner(owner) {
        reset();
    }

    Shard_Iterator& Shard_Iterator::operator++() {
            if (!valid || _shard_idx == 0 || _shard_idx >= _owner.num_shards()) {
                valid = false;
            }
            else {
                --_shard_idx;
            }
            return *this;
    }

    Shard_Info* Shard_Iterator::shard() {
        if (_shard_idx < 0 || _shard_idx >= _owner.num_shards())
            valid = false;
        return (valid ? &_owner[_shard_idx] : nullptr);
    }

    void Shard_Iterator::reset() {
        _shard_idx = _owner.num_shards() - 1;
        valid = true;
    }

    // size_t getMinRunSize(size_t N) {
    //     size_t r = 0;
    //     while (N >= 64) {
    //         r |= N & 1;
    //         N >>= 1;
    //     }
    //     return N + r;
    // }

    // void Compute_Node_Info::shard_insertionSort(Shard_Info** arr, size_t N, Shard_Info_Pointer_Cmp cmp, bool first) {
    //     for (size_t i = 1; i < N; ++i) {
    //         size_t j;
    //         Shard_Info* temp = arr[i];
    //         for (j = i; (j > 0) && cmp(temp, arr[j-1]); --j) {
    //             arr[j] = arr[j-1];
    //             // if (first)
    //                 // arr[j]->_index = j;
    //         }
    //         arr[j] = temp;
    //         // if (first)
    //             // arr[j]->_index = j;
    //     }
    // }

    // // merge two blocks of sorted arrays -> midx is the starting index of the right array
    // void Compute_Node_Info::shard_merge(size_t lidx, size_t midx, size_t ridx, Shard_Info_Pointer_Cmp cmp, bool first) {
    //     assert(!first || lidx == 0);

    //     size_t nl = midx - lidx, nr = ridx - midx + 1;
    //     Shard_Info** left = new Shard_Info*[nl];
    //     Shard_Info** right = new Shard_Info*[nr];
    //     size_t i, j, k;

    //     // copy the two blocks into two temp arrays
    //     memcpy(left, &_shards[lidx], nl);
    //     memcpy(right, &_shards[midx], nr);

    //     std::cout << "in merg after mecpy: left = " << lidx << ", mid = " << midx << ", right = " << ridx << "\n";

    //     // merge algorithm
    //     i = 0;
    //     j = 0;
    //     k = lidx;
    //     while (i < nl && j < nr) {
    //         std::cout << "k = " << k << ", i = " << i << ", j = " << j << "\n";
    //         if (cmp(left[i], right[j])) {
    //             _shards[k] = left[i];
    //             ++i;
    //         }
    //         else {
    //             _shards[k] = right[j];
    //             ++j;
    //         }

    //         // if (first)
    //             // _shards[k]->_index = k;

    //         ++k;
    //     }


    //     // put the remaining elements in the array
    //     while (i < nl) {
    //         _shards[k] = left[i];
    //         // if (first)
    //             // _shards[k]->_index = k;
    //         ++i;
    //         ++k;
    //     }

    //     while (j < nr) {
    //         _shards[k] = right[j];
    //         // if (first)
    //             // _shards[k]->_index = k;
    //         ++j;
    //         ++k;
    //     }

    //     delete[] left;
    //     delete[] right;

    // }

    // void Compute_Node_Info::set_shard_index() {
    //     // for (size_t i = 0; i < _shards.size(); ++i) {
    //     //     _shards[i]->_index = i;
    //     // }
    // }

    // // bottom-up implementation 
    // void Compute_Node_Info::shard_mergeSort(bool change_idx) {  
    //     Shard_Info_Pointer_Cmp cmp;
    //     size_t N = _shards.size();
    //     size_t currentSize = getMinRunSize(N), i;
    //     std::cout << "CurrentSize = " << currentSize << ", N = " << N << "\n";

    //     // ad-hoc -> sort smaller blocks using insertion sort
    //     if (currentSize > 1) {
    //         for (i = 0; i < N; i += currentSize)
    //             shard_insertionSort(&_shards[i], i + currentSize < N ? currentSize : N - i, cmp, change_idx && currentSize >= N);
    //     }

    //     std::cout << "afetr insertion sort\n";
        
    //     for (; currentSize < N; currentSize *= 2) {
    //         std::cout << "current_sizze = " << currentSize << "\n";
    //         for (i = 0; i < N - 1; i += 2*currentSize) {
    //             size_t mid = std::min(i + currentSize, N-1);
    //             size_t right = std::min(i + 2*currentSize - 1, N-1);
    //             std::cout << "left = " << i << ", mid = " << mid << ", right = " << right << "\n";

    //             if (cmp(_shards[mid], _shards[mid - 1])) // merge two blocks if they are not already sorted
    //                 shard_merge(i, mid, right, cmp, currentSize * 2 >= N);
    //             else if (currentSize * 2 >= N)
    //                 set_shard_index();
    //         }
    //     }
    // }

    void Compute_Node_Info::sort_shards_if_needed() {
        if (!is_sorted) {
            // shard_mergeSort();
            std::sort(_shards.begin(), _shards.end(), Shard_Info_Pointer_Cmp{});
            is_sorted = true;
        }
    }

    Load_Info_Container::Load_Info_Container(size_t num_compute, size_t num_shards_per_compute) 
        : cnodes(num_compute), shards(num_compute * num_shards_per_compute) {
        // std::cout << num_compute << " " << num_shards_per_compute << "\n";

        size_t shard_id = 0;
        for (size_t i = 0; i < num_compute; ++i) {
            cnodes[i]._id = i;
            cnodes[i]._shards.resize(num_shards_per_compute);
            cnodes[i].itr.reset();
            for (size_t j = 0; j < num_shards_per_compute; ++j, ++shard_id) {
                shards[shard_id]._id = shard_id;
                // shards[shard_id]._index = j;
                shards[shard_id]._owner = i;
                cnodes[i]._shards[j] = &shards[shard_id];
            }
        }

    }

    Load_Info_Container::~Load_Info_Container() {}

    // void Load_Info_Container::rewrite_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) {
    //     Shard_Info& _shard = shard_id(shard);
    //     if (mu.try_lock_shared()) {
    //         _shard._load.num_reads.store(num_reads);
    //         _shard._load.num_writes.store(num_writes);
    //         _shard._load.num_remote_reads.store(num_remote_reads);
    //         _shard._load.num_flushes.store(num_flushes);
    //         mu.unlock_shared();
    //     }
    //     else {
            // _shard._load.temp_num_reads.store(num_reads);
            // _shard._load.temp_num_writes.store(num_writes);
            // _shard._load.temp_num_remote_reads.store(num_remote_reads);
            // _shard._load.temp_num_flushes.store(num_flushes);
    //     }
    // }

    // void Load_Info_Container::increment_load_info(size_t shard, size_t num_reads, size_t num_writes, size_t num_remote_reads, size_t num_flushes) { 
    //     // TODO add memory order, compute load increment instead to have only one FAA? maybe even just send the new load from cnode, if we use just the new load
    //     // we may be able to remove the mutex and use the atomic -> it should not affect our lb that much but it is much more efficient
    //     Shard_Info& _shard = shard_id(shard);
    //     if (mu.try_lock_shared()) {
    //         _shard._load.num_reads.fetch_add(num_reads);
    //         _shard._load.num_writes.fetch_add(num_writes);
    //         _shard._load.num_remote_reads.fetch_add(num_remote_reads);
    //         _shard._load.num_flushes.fetch_add(num_flushes);
    //         mu.unlock_shared();
    //     }
    //     else {
    //         _shard._load.temp_num_reads.fetch_add(num_reads);
    //         _shard._load.temp_num_writes.fetch_add(num_writes);
    //         _shard._load.temp_num_remote_reads.fetch_add(num_remote_reads);
    //         _shard._load.temp_num_flushes.fetch_add(num_flushes);
    //     }
    // }

    void Load_Info_Container::increment_load_info(size_t shard, size_t added_load) { 
        // TODO add memory order
        Shard_Info& _shard = shard_id(shard);
        _shard._load.current_load.fetch_add(added_load);
    }

    // void Load_Info_Container::flush_load_changes() { // TODO add memory order
    //     for(auto& shard : shards) {
    //         shard._load.num_reads.fetch_add(shard._load.temp_num_reads.load());
    //         shard._load.num_writes.fetch_add(shard._load.temp_num_writes.load());
    //         shard._load.num_remote_reads.fetch_add(shard._load.temp_num_remote_reads.load());
    //         shard._load.num_flushes.fetch_add(shard._load.temp_num_flushes.load());

    //         shard._load.temp_num_reads.store(0);
    //         shard._load.temp_num_writes.store(0);
    //         shard._load.temp_num_remote_reads.store(0);
    //         shard._load.temp_num_flushes.store(0);
    //     }
    // }

    void Load_Info_Container::compute_load_and_pass(size_t& min_load, size_t& max_load, size_t& mean_load) {
        // mu.lock();
        updates.clear();
        ordered_nodes.clear();
        max_load_change = 0;
        cnodes[0].compute_load_and_pass();
        // std::cout << "done with comuting load\n";
        mean_load = cnodes[0]._overal_load;
        min_load = cnodes[0]._overal_load;
        max_load = cnodes[0]._overal_load;
        ordered_nodes.insert({cnodes[0]._overal_load, 0});
        for (size_t i = 1; i < cnodes.size(); ++i) {
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
        // mu.unlock();

        // flush_load_changes();

        mean_load /= cnodes.size();
    }

    void Load_Info_Container::update_max_load() {
        if (max_load_change == 0)
            return;

        Compute_Node_Info& _max_node = max_node();
        assert(_max_node._overal_load > max_load_change);
        _max_node._overal_load -= max_load_change;
        ordered_nodes.erase(std::prev(ordered_nodes.end()));
        ordered_nodes.insert({_max_node._overal_load, _max_node._id});
        max_load_change = 0;
    }

    void Load_Info_Container::change_owner_from_max_to_min(size_t shard_idx) {
        Compute_Node_Info& from = max_node();
        Compute_Node_Info& to = min_node();
        Shard_Info& shard = from[shard_idx];
        // std::cout << "changing ownership of shard " << shard.id() << " from " << from.id() << " (load: " << from.load() - max_load_change 
        //     << " -> " << from.load() - max_load_change - shard._load.last_load << ") to " << to.id() << "\n" ;
        assert(shard._owner == from._id);
        shard._owner = to._id;
        to._overal_load += shard._load.last_load;
        updates.push_back({from._id, to._id, shard_idx});
        ordered_nodes.erase(ordered_nodes.begin());
        ordered_nodes.insert({to._overal_load, to._id});
        max_load_change += shard._load.last_load;
    }

    std::vector<Owner_Ship_Transfer> Load_Info_Container::apply() {
        for (auto update : updates) {
            size_t to = update.to;
            size_t shard_id = cnodes[update.from][update.shard].id();

            cnodes[to]._shards.push_back(&shards[shard_id]);
            cnodes[to].is_sorted = false;
            shards[shard_id]._owner = to;
            // shards[shard_id]._index = cnodes[to].num_shards() - 1;
        }

        for (auto& update : updates) {
            size_t from = update.from;
            size_t shard_index = update.shard;
            size_t id = cnodes[update.from][shard_index].id();

            std::swap(cnodes[from]._shards[shard_index], cnodes[from]._shards.back());
            // cnodes[from]._shards[shard_index]->_index = shard_index;
            cnodes[from]._shards.pop_back();
            cnodes[from].is_sorted = false;

            update.shard = id;
        }

        return updates;
    }
}
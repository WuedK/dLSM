#include "load_balancer.h"
#include "../util/random.h"
#include "../util/testlog.h"

#include <iostream>
#include <thread>
#include <stdio.h>
#include <unistd.h>
#include <mutex>
#include <assert.h>

using namespace std;

#define KEY_LB 0ull
// #define KEY_UB 1024ull * 1024ull * 1024ull
// #define LOG_KEY_UB 30ull
#define KEY_UB 1024ull
#define LOG_KEY_UB 10ull

#define RW_P 25

static constexpr size_t local_read_time = 1;
static constexpr size_t remote_read_time = 10;
static constexpr size_t local_write_time = 1;
static constexpr size_t flush_time = 100;
static constexpr size_t mem_table_cap_mb = 64;

struct Input {
    size_t num_compute;
    size_t num_shard_per_compute;
};

void help() {
    // cout << "enter num_compute and num_shard_per_compute respectively\n";
    LOGF(stdout, "enter num_compute and num_shard_per_compute respectively\n");
}

void parse_input(Input& input) {
    help();
    cin >> input.num_compute >> input.num_shard_per_compute;
}

size_t key_to_shard(size_t key, size_t num_shards) {
    size_t num_keys = KEY_UB - KEY_LB;
    size_t shard_size = num_keys / num_shards;
    assert(key / shard_size < num_shards);
    return key / shard_size;
}

struct load_batch {
    std::atomic<size_t> num_reads{0};
    std::atomic<size_t> num_writes{0};
    std::atomic<size_t> num_r_reads{0};
    std::atomic<size_t> num_flushes{0};

    size_t shard_id;
};

void send_info(load_batch& load, TimberSaw::Load_Balancer& lb) {
    while(true) {
        usleep(100000);
        size_t added_load = load.num_reads * local_read_time 
                            + load.num_r_reads * remote_read_time 
                            + load.num_writes * local_write_time 
                            + load.num_flushes * flush_time;

        if (added_load > 0) {
            load.num_reads = 0;
            load.num_r_reads = 0;
            load.num_writes = 0;
            load.num_flushes = 0;
            lb.increment_load_info(load.shard_id, added_load);
        }
    }
}

void load_generator(TimberSaw::Load_Balancer& lb, const Input& in, std::vector<load_batch>& loads) {
    
    TimberSaw::Random32 type_gen(65406);
    TimberSaw::Random64 key_gen(65406);
    TimberSaw::Random32 remote_gen(65406);
    // char buffer[300] = "";
    // std::vector<load_batch> loads(in.num_compute * in.num_shard_per_compute);

    for (int round = 0;; ++round) {
        // cout << round << " " ;
        if (round % 10000 == 0)
            usleep(10);

        // sprintf(buffer + strlen(buffer), "round: %d -> num_queries: %lu \n", round, num_queries);
        // cout << num_queries << "\n";
        // if (round % 30 == 0) {
        //     lb.print(buffer);
        // }

        // LOGF(stdout, "%s", buffer);
        // memset(buffer, 0, 300);

        // while(--num_queries) {
            // if (round == 28) {
            //     std::cout << num_queries << "\n";
            // }

        size_t key = key_gen.Skewed(LOG_KEY_UB);
        size_t shard = key_to_shard(key, in.num_compute * in.num_shard_per_compute);

        assert(key < KEY_UB);
        assert(shard < loads.size());

        if (type_gen.Uniform(100) < RW_P) {
            loads[shard].num_reads.fetch_add(1);
            loads[shard].num_r_reads.fetch_add((remote_gen.Next() % 100) == 1);
            // ++(loads[shard].num_reads);
            // loads[shard].num_r_reads.f ((remote_gen.Next() % 100) == 1);
        }
        else {
            loads[shard].num_writes.fetch_add(1);
            loads[shard].num_flushes.fetch_add((remote_gen.Next() % 1000) == 1);
            // ++(loads[shard].num_writes);
            // loads[shard].num_flushes += ((remote_gen.Next() % 1000) == 1);
        }
        // }

        
        // if (round == 28) {
        //     std::cout << "bye\n";
        // }
    }
}

void printer(TimberSaw::Load_Balancer& lb) {
    TimberSaw::Random64 num_gen(65406);
    char buffer[30000] = "";

    for (int round = 0;; ++round) {
        sleep(1);
        sprintf(buffer + strlen(buffer), "round: %d\n", round);
        // cout << num_queries << "\n";
        if (round % 1 == 0) {
            lb.print(buffer);
        }

        LOGF(stdout, "%s", buffer);
        memset(buffer, 0, 30000);
    }
}

int main() {
    Input in;
    parse_input(in);
    std::vector<load_batch> loads(in.num_compute * in.num_shard_per_compute);
    TimberSaw::Load_Balancer lb(in.num_compute, in.num_shard_per_compute);
    std::thread t1(printer, std::ref(lb));
    for (size_t i = 0; i < in.num_compute * in.num_shard_per_compute; ++i) {
        // std::cout << i << " hi\n";
        loads[i].shard_id = i;
        std::thread t2(send_info, std::ref(loads[i]), std::ref(lb));
        t2.detach();
    }
    std::thread t3(load_generator, std::ref(lb), std::ref(in), std::ref(loads));
    
    lb.start();
}
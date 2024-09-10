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
#define KEY_UB 1024ull * 1024ull * 1024ull
#define RW_P 25

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

void load_generator(TimberSaw::Load_Balancer& lb, const Input& in) {
    TimberSaw::Random64 num_gen(65406);
    TimberSaw::Random32 type_gen(65406);
    TimberSaw::Random64 key_gen(65406);
    TimberSaw::Random32 remote_gen(65406);
    char buffer[300] = "";

    for (int round = 0;; ++round) {
        // cout << round << " " ;
        size_t num_queries = num_gen.Uniform(1000000ull) + 100000ull;
        sleep(1);
        sprintf(buffer + strlen(buffer), "round: %d -> num_queries: %lu \n", round, num_queries);
        // cout << num_queries << "\n";
        if (round % 30 == 0) {
            lb.print(buffer);
        }

        LOGF(stdout, "%s", buffer);
        memset(buffer, 0, 300);

        while(--num_queries) {
            // if (round == 28) {
            //     std::cout << num_queries << "\n";
            // }

            size_t key = key_gen.Skewed(30);
            assert(key < KEY_UB);
            if (type_gen.Uniform(100) < RW_P) {
                lb.increment_load_info(key_to_shard(key, in.num_compute * in.num_shard_per_compute), 1, 0, (remote_gen.Next() % 100) == 1, 0);
            }
            else {
                lb.increment_load_info(key_to_shard(key, in.num_compute * in.num_shard_per_compute), 0, 1, 0, (remote_gen.Next() % 1000) == 1);
            }
        }
        // if (round == 28) {
        //     std::cout << "bye\n";
        // }
    }
}

int main() {
    Input in;
    parse_input(in);
    TimberSaw::Load_Balancer lb(in.num_compute, in.num_shard_per_compute);
    std::thread t(load_generator, std::ref(lb), std::ref(in));
    
    lb.start();
}
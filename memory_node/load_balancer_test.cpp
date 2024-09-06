#include "load_balancer.h"

#include <random>
#include <iostream>
#include <thread>
#include <stdio.h>
#include <unistd.h>
#include <mutex>

using namespace std;

struct Input {
    size_t num_compute;
    size_t num_shard_per_compute;
};

void help() {
    cout << "enter num_compute and num_shard_per_compute respectively\n";
}

void parse_input(Input& input) {
    help();
    cin >> input.num_compute >> input.num_shard_per_compute;
}

void load(TimberSaw::Load_Balancer& lb, std::mutex& lock, const Input& in) {
    for (int round = 0;; ++round) {
        sleep(1);
        lock.lock();
        if (round % 15 == 0) {
            cout << "round " << round << "\n";
            lb.print();
        }

        for (size_t shard = 0; shard < in.num_compute * in.num_shard_per_compute; ++shard) {
            size_t num_reads = rand() % (10000 + shard*1000);
            size_t num_writes = rand() % (100000 + shard*1000);
            switch (shard % 3)
            {
            case 0:
                num_reads *= 2;
                num_writes *= 2;
                break;
            case 1:
                num_reads /= 4;
                num_writes /= 4;
            default:
                break;
            }
            size_t num_remote_read = num_reads / (rand() % 1000 + 1);
            size_t num_flushes = num_writes / 100;
            lb.increment_load_info(shard, num_reads, num_writes, num_remote_read, num_flushes);
        }
        // if (round % 15 == 0) {
        //     lb.print();
        // }
        lock.unlock();
    }
}

int main() {
    Input in;
    parse_input(in);
    mutex lock;
    TimberSaw::Load_Balancer lb(in.num_compute, in.num_shard_per_compute, lock);
    std::thread t(load, std::ref(lb), std::ref(lock), std::ref(in));
    
    lb.start();
}
#pragma once

#include <vector>
#include <list>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <map>
#include "utils.h"
#include "MurmurHash3.h"
using namespace std;

#define BLOOM_FILTER_SIZE 10240
#define INIT_MEM 10240 + 32
#define MAX_MEM 2 * 1024 * 1024

extern uint64_t time_stamp;
extern int fileID;

class SSTable
{
private:
    struct buffer
    {
        string file_path;
        uint64_t stamp = 0;
        uint64_t quantity = 0;
        uint64_t min_key = UINT64_MAX;
        uint64_t max_key = 0;

        bool bloom_filter[BLOOM_FILTER_SIZE] = {0};
        vector<uint64_t> keys;
        vector<uint32_t> offsets;
    };
    vector<list<buffer> > levels;
    map<int, pair<int, int>> config;
    string cur_dir;
    bool exist(buffer buf, uint64_t key);
    static bool cmp(buffer x, buffer y);
    void load_buf(int level);
    void load_data(const string &file_path, pair<uint64_t, list<pair<uint64_t, string>>> &pairs_with_stamp);
    void to_sst(int level, buffer buf, const list<pair<uint64_t, string>> &data);
    int binary_search(const vector<uint64_t> &keys, uint64_t key, uint64_t l, uint64_t r);
public:
    SSTable(const string &dir);
    bool search(uint64_t key, string &ret);
    void compact();
    void merge(const vector<pair<uint64_t, list<pair<uint64_t, string>>>> &all_pairs_with_stamp, list<pair<uint64_t, string>> &ret, bool flag);
    void reset();
    void updatelvl0();
};
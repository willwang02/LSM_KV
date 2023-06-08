#pragma once

#include <vector>
#include "MurmurHash3.h"
#include "utils.h"
#include <fstream>
#include <iostream>

#define BLOOM_FILTER_SIZE 10240
#define MAXL 10
#define MAX_MEM 2 * 1024 * 1024
#define INIT_MEM 10240 + 32

using namespace std;
extern uint64_t time_stamp;
extern int fileID;

class MemTable
{
private:
    struct node
    {
        uint64_t key = 0;
        string value = "";
        node* next[MAXL];
        node(uint64_t newk = 0, const std::string &newv = "") : key(newk), value(newv) {}
    } *head;
    
    int randomL();
    node *update[MAXL];
    string cur_dir;
    uint64_t size = INIT_MEM;

    //Header
    // uint64_t time_stamp = 0;
    uint64_t quantity = 0;
    uint64_t min_key = UINT64_MAX;
    uint64_t max_key = 0;

    bool bloom_filter[BLOOM_FILTER_SIZE];
public:
    MemTable(const string &dir);
    void init();
    bool search(uint64_t searchKey, string &ret);
    bool insert(uint64_t searchKey, const string &value);
    void to_sst();
};
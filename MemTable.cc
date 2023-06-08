#include "MemTable.h"


MemTable::MemTable(const string &dir)
{
    cur_dir = dir;
    init();
}
void MemTable::init() {
    this->head = new node(0, "");
    this->quantity = 0;
    this->min_key = UINT64_MAX;
    this->max_key = 0;
    size = INIT_MEM;
    for (int i = 0; i < MAXL; ++i) {
        head->next[i] = NULL;
    }
}
int MemTable::randomL()
{
    int newL = 0;
    while((double) rand() / RAND_MAX < 0.5 && newL + 1 < MAXL)
        ++newL;
    return newL + 1;
}
bool MemTable::insert(uint64_t searchKey, const string &value)
{
    // std::cout << "insert" << std::endl;
    node *x = head;
    for(int i = MAXL - 1; i >= 0;--i)
    {
        while(x->next[i] != NULL && x->next[i]->key < searchKey)
            x = x->next[i];
        update[i] = x;
    }
    if(x->next[0] != NULL && x->next[0]->key == searchKey)
    {
        x = x->next[0];
        if(size + value.length() - x->value.length() > MAX_MEM)
        {
            to_sst();
            init();
            insert(searchKey, value);
            return true;
        }
        x->value = value;
        size += value.length() - x->value.length();
    }
    else
    {
        if(size + value.length() + 8 + 4 + 1 > MAX_MEM)
        {
            to_sst();
            init();
            insert(searchKey, value);
            return true;
        }
        int newL = randomL();
        x = new node(searchKey, value);
        for(int i = 0;i < newL;++i)
        {
            x->next[i] = update[i]->next[i];
            update[i]->next[i] = x;
        }
        ++quantity;
        size += value.length() + 8 + 4 + 1;
        min_key = min(min_key, searchKey);
        max_key = max(max_key, searchKey);
    }
    return false;
}
bool MemTable::search(uint64_t searchKey, string &ret)
{
    // std::cout << "search" << std::endl;
    node *x = head;
    // int ret = 0;
    for(int i = MAXL - 1; i >= 0;--i)
        while(x->next[i] != NULL && x->next[i]->key < searchKey)
            x = x->next[i];
    // --ret;
    if(x->next[0]!= NULL && x->next[0]->key == searchKey)
    {
        x = x->next[0];
        ret = x->value == "~DELETED~" ? "" : x->value;
        return true;
    }
    return false;
}
void MemTable::to_sst()
{
    // std::cout << "to_sst" << std::endl;
    if(quantity == 0)
        return;
    string file_path = cur_dir + "/level-0";

    if(!utils::dirExists(file_path))
        utils::mkdir(file_path.c_str());
    
    node *x = head;
    while(x->next[0] != NULL)
    {
        x = x->next[0];
        unsigned int hash[4] = {0};
        MurmurHash3_x64_128(&x->key, 8, 1, hash);
        bloom_filter[hash[0] % BLOOM_FILTER_SIZE] = 1;
        bloom_filter[hash[1] % BLOOM_FILTER_SIZE] = 1;
        bloom_filter[hash[2] % BLOOM_FILTER_SIZE] = 1;
        bloom_filter[hash[3] % BLOOM_FILTER_SIZE] = 1;
    }

    ofstream fout(file_path + "/" + to_string(++time_stamp) + "-" + to_string(++fileID) + ".sst", ios::out | ios::binary);
    fout.write((char*)(&time_stamp), 8);
    fout.write((char*)(&quantity), 8);
    fout.write((char*)(&min_key), 8);
    fout.write((char*)(&max_key), 8);
    fout.write((char*)(bloom_filter), BLOOM_FILTER_SIZE);
    
    x = head;
    uint32_t offset = INIT_MEM + quantity * 12;
    while(x->next[0] != NULL)
    {
        x = x->next[0];
        fout.write((char*)(&x->key), 8);
        fout.write((char*)(&offset), 4);
        offset += x->value.length() + 1;
    }
    x = head;
    while(x->next[0] != NULL)
    {
        x = x->next[0];
        fout << x->value << '\0';
    }
    fout.close();
}
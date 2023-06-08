#include "kvstore.h"


uint64_t time_stamp = 0;
int fileID = 0;
KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir)
{
    time_stamp = 0;
    fileID = 0;
    this->dir = dir;
    sstable = new SSTable(dir);
    memtable = new MemTable(dir);
}

KVStore::~KVStore()
{
    memtable->to_sst();
    sstable->updatelvl0();
    sstable->compact();
}

void KVStore::put(uint64_t key, const std::string &s)
{
    if(memtable->insert(key, s))
    {
        sstable->updatelvl0();
        sstable->compact();
    }
}

std::string KVStore::get(uint64_t key)
{
    string ret;
    // if(key == 2030)
    //     cout << 1;
    if(memtable->search(key, ret))
        return ret;
    if(sstable->search(key, ret))
        return ret;
    return "";
}

bool KVStore::del(uint64_t key)
{
    string value = get(key);
    if(value == "")
        return false;
    put(key, "~DELETED~");
    return true;
}

void KVStore::reset()
{
    sstable->reset();
    delete memtable;
    delete sstable;
    time_stamp = 0;
    fileID = 0;
    sstable = new SSTable(dir);
    memtable = new MemTable(dir);
}
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list)
{
    
}
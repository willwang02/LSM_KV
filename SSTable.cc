#include "SSTable.h"

bool SSTable::exist(SSTable::buffer buf, uint64_t key)
{
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, 8, 1, hash);
    return buf.bloom_filter[hash[0] % BLOOM_FILTER_SIZE] && buf.bloom_filter[hash[1] % BLOOM_FILTER_SIZE] \
    && buf.bloom_filter[hash[2] % BLOOM_FILTER_SIZE] && buf.bloom_filter[hash[3] % BLOOM_FILTER_SIZE];
}
bool SSTable::cmp(SSTable::buffer x, SSTable::buffer y)
{
    return x.stamp < y.stamp || x.min_key < y.min_key;
}
SSTable::SSTable(const string &dir)
{
    // std::cout << "flag" << std::endl;
    cur_dir = dir;
    ifstream fin("default.conf");
    int lvl, max_num;
    string mode;
    while(fin >> lvl >> max_num >> mode)
        config.insert(make_pair(lvl , make_pair(max_num, mode == "Tiering" ? 0 : 1)));
    load_buf(-1);
    // std::cout << "flag2" << std::endl;
}
void SSTable::load_buf(int level)
{
    // std::cout << "load_buf" << std::endl;
    if (level == -1)
    {
        levels.clear();
        vector<string> level_dirs;
        int level_num = utils::scanDir(cur_dir, level_dirs);
        for (int i = 0; i < level_num; ++i)
        {
            vector<string> files;
            int file_num = utils::scanDir(cur_dir + "/" + level_dirs[i], files);
            list<buffer> bufs;
            for (int j = 0; j < file_num; ++j)
            {
                buffer buf;
                buf.file_path = cur_dir + "/" + level_dirs[i] + "/" + files[j];
                ifstream fin(buf.file_path, ios::in | ios::binary);

                fin.read((char*)(&buf.stamp), 8);
                fin.read((char*)(&buf.quantity), 8);
                fin.read((char*)(&buf.min_key), 8);
                fin.read((char*)(&buf.max_key), 8);
                fin.read((char*)(&buf.bloom_filter), BLOOM_FILTER_SIZE);

                for (int k = 0; k < buf.quantity; ++k)
                {
                    uint64_t key, offset;
                    fin.read((char *)(&key), 8);
                    fin.read((char *)(&offset), 4);
                    buf.keys.push_back(key);
                    buf.offsets.push_back(offset);
                }
                bufs.push_back(buf);
                if (buf.stamp > time_stamp)
                    time_stamp = buf.stamp;
            }
            bufs.sort(cmp);
            levels.push_back(bufs);
        }
        ++time_stamp;
    }
    else
    {
        vector<string> files;
        int file_num = utils::scanDir(cur_dir + "/level-" + to_string(level), files);
        list<buffer> bufs;
        for (int j = 0; j < file_num; ++j)
        {
            buffer buf;
            buf.file_path = cur_dir + "/level-" + to_string(level) + "/" + files[j];
            ifstream fin(buf.file_path, ios::in | ios::binary);

            fin.read((char *)(&buf.stamp), 8);
            fin.read((char *)(&buf.quantity), 8);
            fin.read((char *)(&buf.min_key), 8);
            fin.read((char *)(&buf.max_key), 8);
            fin.read((char *)(&buf.bloom_filter), BLOOM_FILTER_SIZE);

            for (int k = 0; k < buf.quantity; ++k)
            {
                uint64_t key, offset;
                fin.read((char *)(&key), 8);
                fin.read((char *)(&offset), 4);
                buf.keys.push_back(key);
                buf.offsets.push_back(offset);
            }
            bufs.push_back(buf);
        }
        bufs.sort(cmp);
        if (level == levels.size())
            levels.push_back(bufs);
        else
        {
            levels[level].clear();
            levels[level] = bufs;
        }
    }
}
int SSTable::binary_search(const vector<uint64_t> &keys, uint64_t key, uint64_t l, uint64_t r)
{
    while(l != r)
    {
        uint64_t mid = (l + r) / 2;
        if(keys[mid] >= key)
            r = mid;
        else
            l = mid + 1;
    }
    if(keys[l] == key)
        return l;
    return -1;
}
bool SSTable::search(uint64_t key, string &ret)
{
    // std::cout << "sst_search" << std::endl;
    vector<uint64_t> stamps;
    vector<string> values;
    int pos = 0;
    for (const auto &level : levels)
        for (auto &buf : level)
            if (buf.min_key <= key && key <= buf.max_key \
            && exist(buf, key) \
            && (pos = binary_search(buf.keys, key, 0, buf.quantity - 1)) != -1)
            {
                string value;
                ifstream fin(buf.file_path);
                fin.seekg(buf.offsets[pos]);
                getline(fin, value, '\0');
                stamps.push_back(buf.stamp);
                values.push_back(value);
            }
    if (values.empty())
        return false;
    int max_pos = 0;
    for (int i = 0; i < stamps.size(); ++i)
        if (stamps[i] > stamps[max_pos])
            max_pos = i;
    ret = values[max_pos];
    if (ret == "~DELETED~")
        return false;
    return true;
}

void SSTable::load_data(const string &file_path, pair<uint64_t, list<pair<uint64_t, string>>> &pairs_with_stamp)
{
    // std::cout << "sst_load_data" << std::endl;
    uint64_t stamp;
    uint64_t quantity;
    ifstream fin(file_path, ios::in | ios::binary);

    fin.read((char *)(&stamp), 8);
    fin.read((char *)(&quantity), 8);
    fin.seekg(16 + BLOOM_FILTER_SIZE, ios::cur);

    pairs_with_stamp.first = stamp;
    pairs_with_stamp.second.clear();
    // std::cout <<quantity<<std::endl;
    for (int i = 0; i < quantity; ++i)
    {
        // std::cout << "a_flag2" << std::endl;
        uint64_t key;
        fin.read((char *)(&key), 8);
        fin.seekg(4, ios::cur);
        pairs_with_stamp.second.push_back(make_pair(key, ""));
        // std::cout << "a_flag3" << std::endl;
    }
    
    for (auto i = pairs_with_stamp.second.begin(); i != pairs_with_stamp.second.end(); ++i)
    {
        string data;
        getline(fin, data, '\0');
        i->second = data;
    }
}
void SSTable::merge(const vector<pair<uint64_t, list<pair<uint64_t, string>>>> &_all_pairs_with_stamp, list<pair<uint64_t, string>> &ret, bool flag)
{
    // std::cout << "sst_merge" << std::endl;
    // pairs_with_stamp.sort();
    vector<pair<uint64_t, list<pair<uint64_t, string>>>> all_pairs_with_stamp;
    all_pairs_with_stamp.assign(_all_pairs_with_stamp.begin(), _all_pairs_with_stamp.end());
    sort(all_pairs_with_stamp.begin(), all_pairs_with_stamp.end());
    list<pair<uint64_t, string>> tmp2;
    ret.assign(all_pairs_with_stamp.begin()->second.begin(), all_pairs_with_stamp.begin()->second.end());
    for (auto it = all_pairs_with_stamp.begin() + 1; it != all_pairs_with_stamp.end(); ++it)
    {
        auto it_2_1 = it->second.begin();
        auto it_2_2 = ret.begin();
        while (it_2_1 != it->second.end() || it_2_2 != ret.end())
        {

            if (it_2_1 != it->second.end() && it_2_2 != ret.end() && it_2_1->first < it_2_2->first || it_2_1 != it->second.end() && it_2_2 == ret.end())
            {
                // cout << it_2_1->first <<" "<< it_2_1->second <<endl;
                tmp2.push_back(*it_2_1);
                ++it_2_1;
            }
            else if (it_2_1 != it->second.end() && it_2_2 != ret.end() && it_2_1->first > it_2_2->first || it_2_1 == it->second.end() && it_2_2 != ret.end())
            {
                // cout << it_2_2->first <<" "<< it_2_2->second <<endl;
                tmp2.push_back(*it_2_2);
                ++it_2_2;
            }
            else if (it_2_1 != it->second.end() && it_2_2 != ret.end() && it_2_1->first == it_2_2->first)
            {
                // cout << it_2_1->first <<" "<< it_2_1->second <<endl;
                tmp2.push_back(*it_2_1);
                ++it_2_1;
                ++it_2_2;
            }
        }
        tmp2.swap(ret);
        tmp2.clear();
    }
    // for (auto it = ret.begin(); it != ret.end(); ++it)
    //     cout << it->first <<" "<<it->second<<endl;
    if (flag)
        for (auto it = ret.begin(); it != ret.end(); ++it)
            if (it->second == "~DELETED~")
                it = ret.erase(it);
}
void SSTable::compact()
{
    // std::cout << "sst_compact" << std::endl;
    for (int i = 0; i < levels.size(); ++i)
        if (config.find(i) != config.end() && levels[i].size() > config.at(i).first || config.find(i) == config.end() && levels[i].size() > 2 * (i + 1))
        {
            vector<pair<uint64_t, list<pair<uint64_t, string>>>> all_pairs_with_stamp;
            pair<uint64_t, list<pair<uint64_t, string>>> pairs_with_stamp;
            list<string> files;
            uint64_t min_key = levels[i].begin()->min_key, max_key = levels[i].begin()->max_key;
            int file_num;
            if(config.find(i) != config.end())
                file_num = (config.at(i).second == 0) ? (levels[i].size()) : (levels[i].size() - config.at(i).first);
            else
                file_num = (i == 0) ? (levels[i].size()) : (levels[i].size() - 2 * (i + 1));
            auto _it = levels[i].begin();
            int max_stamp = 0;
            for (int j = 0; j < file_num; ++j)
            {
                if (_it->stamp > max_stamp)
                    max_stamp = _it->stamp;
                load_data(_it->file_path, pairs_with_stamp);
                all_pairs_with_stamp.push_back(pairs_with_stamp);

                min_key = min(min_key, _it->min_key);
                max_key = max(max_key, _it->max_key);
                files.push_back(_it->file_path);
                ++_it;
                levels[i].pop_front();
            }
            int overlap_cnt = 0;
            if (i + 1 < levels.size())
                for (auto it = levels[i + 1].begin(); it != levels[i + 1].end();)
                {
                    if (it->max_key < min_key || it->min_key > max_key)
                        ++it;
                    else
                    {
                        if (it->stamp > max_stamp)
                            max_stamp = it->stamp;
                        load_data(it->file_path, pairs_with_stamp);
                        all_pairs_with_stamp.push_back(pairs_with_stamp);
                        files.push_back(it->file_path);
                        it = levels[i + 1].erase(it);
                        ++overlap_cnt;
                    }
                }

            list<pair<uint64_t, string>> ret;
            merge(all_pairs_with_stamp, ret, i == levels.size() - 1);
            for (auto it = ret.begin(); it != ret.end();)
            {
                list<pair<uint64_t, string>> data;
                buffer buf;
                buf.stamp = max_stamp;
                buf.file_path = cur_dir + "/level-" + to_string(i + 1) + "/" + to_string(buf.stamp) + "-" + to_string(++fileID) + ".sst";
                buf.min_key = it->first;
                buf.max_key = it->first;
                uint32_t size = INIT_MEM;
                while (it != ret.end() && size + it->second.length() + 1 + 12 <= MAX_MEM)
                {
                    data.push_back(*it);
                    buf.min_key = min(buf.min_key, it->first);
                    buf.max_key = max(buf.max_key, it->first);
                    ++buf.quantity;
                    size += it->second.length() + 1 + 12;
                    ++it;
                }
                for (auto it : data)
                {
                    unsigned int hash[4] = {0};
                    MurmurHash3_x64_128(&it.first, 8, 1, hash);
                    buf.bloom_filter[hash[0] % BLOOM_FILTER_SIZE] = 1;
                    buf.bloom_filter[hash[1] % BLOOM_FILTER_SIZE] = 1;
                    buf.bloom_filter[hash[2] % BLOOM_FILTER_SIZE] = 1;
                    buf.bloom_filter[hash[3] % BLOOM_FILTER_SIZE] = 1;
                }
                to_sst(i + 1, buf, data);
                uint32_t offset = INIT_MEM + 12 * buf.quantity;
                for (auto kv : data)
                {
                    buf.keys.push_back(kv.first);
                    buf.offsets.push_back(offset);
                    offset += kv.second.length() + 1;
                }
                if (i == levels.size() - 1)
                {
                    list<buffer> bufs;
                    levels.push_back(bufs);
                }
                levels[i + 1].push_back(buf);
            }
            for (const string &file : files)
                utils::rmfile(file.c_str());
        }
}
void SSTable::to_sst(int level, buffer buf, const list<pair<uint64_t, string>> &data)
{
    if (!utils::dirExists(cur_dir + "/level-" + to_string(level)))
        utils::mkdir((cur_dir + "/level-" + to_string(level)).c_str());
    ofstream fout(buf.file_path, ios::out | ios::binary);

    fout.write((char *)(&buf.stamp), 8);
    fout.write((char *)(&buf.quantity), 8);
    fout.write((char *)(&buf.min_key), 8);
    fout.write((char *)(&buf.max_key), 8);
    fout.write((char *)(buf.bloom_filter), BLOOM_FILTER_SIZE);

    uint32_t offset = INIT_MEM + 12 * buf.quantity;
    for (auto it : data)
    {
        fout.write((char *)(&it.first), 8);
        fout.write((char *)(&offset), 4);
        offset += it.second.length() + 1;
    }
    for (auto it : data)
        fout << it.second << '\0';
    fout.close();
}
void SSTable::reset()
{
    levels.clear();
    vector<string> levels;
    int level_num = utils::scanDir(cur_dir, levels);
    for (int i = 0; i < level_num; ++i)
    {
        vector<string> files;
        int file_num = utils::scanDir(cur_dir + "/" + levels[i], files);
        for (int j = 0; j < file_num; ++j)
            utils::rmfile((cur_dir + "/" + levels[i] + "/" + files[j]).c_str());
        utils::rmdir((cur_dir + "/" + levels[i]).c_str());
    }
}

void SSTable::updatelvl0()
{
    load_buf(0);
}

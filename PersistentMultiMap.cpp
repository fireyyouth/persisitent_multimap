#include <unordered_map>
#include <set>
#include <map>
#include <string_view>
#include <memory>

#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <string.h>

struct FileDiscriptor {
    int fd_;
    FileDiscriptor(int fd) : fd_(fd) {}
    ~FileDiscriptor() {
        if (fd_ >= 0) {
            close(fd_);
        }
    }
    int Get() const {
        return fd_;
    }
};

struct DirDiscriptor {
    DIR *dir_;
    DirDiscriptor(DIR *dir) : dir_(dir) {}
    ~DirDiscriptor() {
        if (dir_) {
            closedir(dir_);
        }
    }
    DIR *Get() const {
        return dir_;
    }
};

struct PersistentMultiMap {
    static PersistentMultiMap & GetInstance() {
        static PersistentMultiMap instance;
        return instance;
    }

    std::map<std::pair<uint64_t, uint64_t>, std::pair<void *, size_t>> mapping_;
    std::map<void *, decltype(mapping_)::iterator> reverse_mapping_;


    uint64_t GetMaxVersion(uint64_t id) {
        auto end = mapping_.upper_bound(std::make_pair(id, 0UL - 1UL));
        if (end == mapping_.begin()) {
            return 0;
        }
        if (std::prev(end, 1)->first.first != id) {
            return 0;
        }
        return std::prev(end, 1)->first.second;
    }


    PersistentMultiMap(const PersistentMultiMap &)=delete;
    PersistentMultiMap(PersistentMultiMap &&)=delete;
    PersistentMultiMap & operator=(const PersistentMultiMap &)=delete;
    PersistentMultiMap & operator=(PersistentMultiMap &&)=delete;

    static bool ParseUInt(const char *s, uint64_t *out) {
        char *end = nullptr;
        auto id = strtoul(s, &end, 10);
        if (!end || *end != '\0') {
            return false;
        }
        *out = id;
        return true;
    }

    PersistentMultiMap() {
        DirDiscriptor root_dir(opendir("/dev/shm"));
        if (!root_dir.Get()) {
            throw 0;
        }
        int root_dir_fd = dirfd(root_dir.Get()); // owned by root_dir
        while (auto dp = readdir(root_dir.Get())) {
            uint64_t id;
            if (!ParseUInt(dp->d_name, &id)) {
                continue;
            }

            int sub_dir_fd = openat(root_dir_fd, dp->d_name, O_RDONLY | O_DIRECTORY);
            if (sub_dir_fd < 0) {
                throw std::runtime_error(strerror(errno));
            }
            DirDiscriptor sub_dir(fdopendir(sub_dir_fd)); // take ownership of sub_dir_fd
            if (!sub_dir.Get()) {
                throw 0;
            }
            while (auto dp = readdir(sub_dir.Get())) {
                uint64_t version;
                if (!ParseUInt(dp->d_name, &version)) {
                    continue;
                }
 
                FileDiscriptor shm_fd(openat(sub_dir_fd, dp->d_name, O_RDWR));
                if (shm_fd.Get() < 0) {
                    throw 0;
                }
                struct stat statbuf;
                fstat(shm_fd.Get(), &statbuf);

                void *addr = mmap(NULL, statbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd.Get(), 0);
                if (!addr) {
                    throw 0;
                }
                auto [iter, inserted] = mapping_.emplace(std::make_pair(id, version), std::make_pair(addr, statbuf.st_size));
                reverse_mapping_.emplace(addr, iter);
            }
        }
    }


    void *Create(uint64_t id, size_t size) {
        char dir_path[300];
        snprintf(dir_path, sizeof(dir_path), "/dev/shm/%lu", id);

        if (mkdir(dir_path, 0777) < 0 && errno != EEXIST) {
            throw 0;
        }

        uint64_t version = 0;
        {
            version = GetMaxVersion(id);
            if (version + 1 == 0) {
                throw 0;
            }
            version += 1;
        }

        void *ret = nullptr;
        {
            char path[300];
            snprintf(path, sizeof(path), "%s/%lu", dir_path, version);

            FileDiscriptor fd(open(path, O_RDWR | O_CREAT | O_EXCL, S_IRUSR | S_IWUSR));
            if (fd.Get() < 0) {
                throw 0; 
            }

            if (ftruncate(fd.Get(), size) < 0) {
                throw 0;
            }

            ret = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd.Get(), 0);
            if (!ret) {
                throw 0;
            }
        }


        auto [iter, inserted] = mapping_.emplace(std::make_pair(id, version), std::make_pair(ret, size));
        if (!inserted) {
            throw 0;
        }
        auto [r_iter, r_inserted] = reverse_mapping_.emplace(ret, iter);
        if (!r_inserted) {
            throw 0;
        }

        return ret;
    }

    void Delete(void *value) {
        auto r_it = reverse_mapping_.find(value);
        if (r_it == reverse_mapping_.end()) {
            throw 0;
        }
        auto it = r_it->second;

        munmap(value, it->second.second);

        {
            char path[300];
            snprintf(path, sizeof(path), "/dev/shm/%lu/%lu", it->first.first, it->first.second);
            unlink(path);
        }

        {
            char dir_path[300];
            snprintf(dir_path, sizeof(dir_path), "/dev/shm/%lu", it->first.first);
            rmdir(dir_path);
        }

        reverse_mapping_.erase(r_it);
        mapping_.erase(it);
    }
};

struct Foo {
    char data[128];
};

struct Bar {
    char data[256];
};

#include <iostream>

void inspect() {
    if (fork() == 0) {
        execl("/usr/bin/tree", "/usr/bin/tree", "/dev/shm", NULL);
    }
    wait(nullptr);
}

int main() {
    try {
        auto & pmap = PersistentMultiMap::GetInstance();
        std::string dumy;

        inspect();

        auto foo = (Foo *)pmap.Create(1, sizeof(Foo));
        memset(foo->data, 'f', sizeof(foo->data));

        inspect();

        auto bar = (Bar *)pmap.Create(1, sizeof(Bar));
        memset(bar->data, 'b', sizeof(bar->data));

        inspect();

        pmap.Delete(foo);

        inspect();

//    pmap.Delete(bar);

 //   inspect();
    } catch (const std::exception & exp) {
        std::cerr << exp.what() << std::endl;
    }
}


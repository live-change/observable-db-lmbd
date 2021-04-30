//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_DATABASE_H
#define OBSERVABLE_DB_LMDB_DATABASE_H

#include <memory>
#include <mutex>
#include <map>
#include <lmdb.h>
#include <uWebSockets/Loop.h>
#include "Store.h"
#include "taskQueue.h"

class Database : public std::enable_shared_from_this<Database> {
private:
  MDB_env* env;
  std::mutex stateMutex;
  std::map<std::string, std::weak_ptr<Store>> stores;
  bool finished = false;
  friend class Store;
public:
  Database(std::string path) {
    mdb_env_create(&env);
    mdb_env_set_mapsize(env, 1024*1024*1024);
    mdb_env_set_maxdbs(env, 10000);
    mdb_env_set_maxreaders(env, 32);
    mdb_env_open(env, path.c_str(), MDB_NOSYNC, 0664);
  }

  void getStore(std::string storeName, std::function<void(std::shared_ptr<Store>)> callback) {
    std::shared_ptr<Database> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    taskQueue.enqueue([storeName, callback, loop, self, this]() {
      std::lock_guard<std::mutex> stateLock(stateMutex);
      if(finished) {
        callback(nullptr);
        return;
      }
      std::shared_ptr<Store> store = nullptr;
      auto it = stores.find(storeName);
      if(it != stores.end()) {
        store = it->second.lock();
      }
      if(store != nullptr) {
        if(store->finished) {
          stores.erase(it);
          store = nullptr;
        } else {
          loop->defer([callback, store]() {
            callback(store);
          });
        }
        return;
      }
      fprintf(stderr, "OPEN OPEN STORE %s\n", storeName.c_str());
      store = std::make_shared<Store>(self, storeName, env);
      int ret = store->open();
      fprintf(stderr, "OPEN OPEN STORE %s RET = %d\n", storeName.c_str(), ret);
      if(ret == 0) {
        stores[storeName] = store;
        loop->defer([callback, store]() {
          callback(store);
        });
      } else {
        loop->defer([callback, store]() {
          callback(nullptr);
        });
      }
    });
  }
  void createStore(std::string name,
                   std::function<void()> onOk, std::function<void(std::string)> onError) {
    std::shared_ptr<Database> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    taskQueue.enqueue([name, onOk, onError, loop, self, this]() {
      std::lock_guard<std::mutex> stateLock(stateMutex);
      if(finished) {
        loop->defer([onError]() {
          onError("closed");
        });
        return;
      }
      auto it = stores.find(name);
      if(it != stores.end()) {
        loop->defer([onError]() {
          onError("exists");
        });
        return;
      }
      std::shared_ptr<Store> store = std::make_shared<Store>(self, name, env);
      fprintf(stderr, "CREATE STORE %s\n", name.c_str());
      int ret = store->create();
      if(ret == 0) {
        stores[name] = store;
        loop->defer([onOk]() {
          onOk();
        });
      } else {
        fprintf(stderr, "MDB CREATE RET %d\n", ret);
        if(ret == MDB_BAD_VALSIZE) {
          loop->defer([onError]() {
            onError("max_bad_valsize");
          });
        } else if (ret == MDB_DBS_FULL) {
          loop->defer([onError]() {
            onError("dbs_full");
          });
        }
      }
    });
  }
  void deleteStore(std::string name,
                   std::function<void()> onOk, std::function<void(std::string)> onError) {
    std::shared_ptr<Database> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    taskQueue.enqueue([name, onOk, onError, loop, self, this]() {
      std::lock_guard<std::mutex> stateLock(stateMutex);
      auto it = stores.find(name);
      std::shared_ptr<Store> store = nullptr;
      if(it != stores.end()) {
        store = it->second.lock();
      }
      if(store == nullptr) {
        store = std::make_shared<Store>(self, name, env);
        if(!store->open()) {
          loop->defer([onError, store]() {
            onError("not_found");
          });
          return;
        }
      }
      store->drop();
      if(it != stores.end()) {
        stores.erase(it);
      }
      loop->defer([onOk]() {
        onOk();
      });
    });
  }

  void close() {
    std::lock_guard lock(stateMutex);
    finished = true;
    for(auto const& [key, val] : stores) {
      std::shared_ptr<Store> store = val.lock();
      store->close();
    }
    mdb_env_sync(env, 1);
    mdb_env_close(env);
  }

};


#endif //OBSERVABLE_DB_LMDB_DATABASE_H

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
#include <nlohmann/json.hpp>

class Database : public std::enable_shared_from_this<Database> {
private:
  MDB_env* env;
  std::mutex stateMutex;
  std::map<std::string, std::weak_ptr<Store>> stores;
  bool finished = false;
  friend class Store;

  static void readFlag(unsigned int& to, nlohmann::json settings, std::string name, unsigned int flag) {
    if(settings.contains(name) && settings[name].is_boolean() && settings[name].get<bool>()) {
      to |= flag;
    } else {
      to &= ~flag;
    }
  }

public:
  Database(std::string path, nlohmann::json settings) {
    mdb_env_create(&env);
    if(settings.contains("mapSize")) mdb_env_set_mapsize(env, settings["mapSize"].get<std::size_t>());
    if(settings.contains("maxDbs")) mdb_env_set_maxdbs(env, settings["maxDbs"].get<std::size_t>());
    if(settings.contains("maxReaders")) mdb_env_set_maxreaders(env, settings["maxReaders"].get<std::size_t>());

    unsigned int flags = 0;
    /// TOP Speed: MDB_NOSYNC | MDB_NOMETASYNC | MDB_WRITEMAP | MDB_NOMEMINIT | MDB_NOTLS
    readFlag(flags, settings, "readOnly", MDB_RDONLY);
    readFlag(flags, settings, "writeMap", MDB_WRITEMAP);
    readFlag(flags, settings, "noMetaSync", MDB_NOMETASYNC);
    readFlag(flags, settings, "noSync", MDB_NOSYNC);
    readFlag(flags, settings, "mapAsync", MDB_MAPASYNC);
    readFlag(flags, settings, "noReadAhead", MDB_NORDAHEAD);
    readFlag(flags, settings, "noMemInit", MDB_NOMEMINIT);

    flags |= MDB_NOTLS; /// needed because async

    mdb_env_set_maxdbs(env, 10000);
    mdb_env_set_maxreaders(env, 32);
    mdb_env_open(env, path.c_str(), flags,0664);
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
      db_log("OPEN OPEN STORE %s", storeName.c_str());
      store = std::make_shared<Store>(self, storeName, env);
      int ret = store->open();
      db_log("OPEN OPEN STORE %s RET = %d", storeName.c_str(), ret);
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
      db_log("CREATE STORE %s", name.c_str());
      int ret = store->create();
      if(ret == 0) {
        stores[name] = store;
        loop->defer([onOk]() {
          onOk();
        });
      } else {
        db_log("MDB CREATE RET %d", ret);
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
        int ret = store->open();
        if(ret != 0) {
          db_log("MDB OPEN %s RET %d", name.c_str(), ret);
          loop->defer([onError, store]() {
            onError("store_not_found");
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
    db_log("closing database stores!");
    for(auto const& [key, val] : stores) {
      std::shared_ptr<Store> store = val.lock();
      db_log("closing store %s", key.c_str());
      if(store != nullptr) store->close();
    }
    db_log("syncing database!");
    mdb_env_sync(env, 1);
    db_log("closing database handle!");
    mdb_env_close(env);
  }

};


#endif //OBSERVABLE_DB_LMDB_DATABASE_H

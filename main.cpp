#include <stdio.h>
#include <boost/program_options.hpp>
#include <lmdb.h>
#include <uWebSockets/App.h>
#include <filesystem>
#include <concurrentqueue/blockingconcurrentqueue.h>
#include "PacketBuffer.h"

namespace options {
  std::string dir;
  int port = 3530;
  int threads = 8;
}

void parseOptions(int argc, char** argv) {
  namespace po = boost::program_options;

  po::options_description desc("Database options");
  desc.add_options()
      ("help,h", "print help message")
      ("dir,d", po::value<std::string>(), "data directory")
      ("port,p", po::value<int>(), "listen port")
      ("threads,t", po::value<int>(), "threads count")
      ;

  po::variables_map vm;
  po::store(po::parse_command_line(argc, argv, desc), vm);
  po::notify(vm);

  if (!vm.count("dir")) {
    fprintf(stderr, "directory parameter is required \n");
    exit(1);
  }

  options::dir = vm["dir"].as<std::string>();

  if (vm.count("port")) options::port = vm["port"].as<int>();
  if (vm.count("threads")) options::port = vm["threads"].as<int>();
}

std::vector<std::thread> lmdbThreadPool;
moodycamel::BlockingConcurrentQueue<std::function<void()>> taskQueue;

void workerThread() {
  while(true) {
    std::function<void()> function;
    taskQueue.wait_dequeue(function);
    function();
  }
}

namespace RangeFlag {
 // static const uint8_t Reverse = 0;
  static const uint8_t Gt = 1;
  static const uint8_t Gte = 2;
  static const uint8_t Lt = 4;
  static const uint8_t Lte = 8;
}

struct RangeView {
  uint8_t flags;
  std::string_view gt;
  std::string_view lt;
  RangeView(net::PacketBuffer& packet) {
    flags = packet.readU8();
    if(flags & (RangeFlag::Gt|RangeFlag::Gte)) {
      int keySize = packet.readU16();
      char* key = packet.readPointer(keySize);
      gt = std::string_view(key, keySize);
    }
    if(flags & (RangeFlag::Lt|RangeFlag::Lte)) {
      int keySize = packet.readU16();
      char* key = packet.readPointer(keySize);
      lt = std::string_view(key, keySize);
    }
  }
};

struct Range {
  uint8_t flags;
  std::string gt;
  std::string lt;
  Range(RangeView rv) {
    flags = rv.flags;
    gt = rv.gt;
    lt = rv.lt;
  }
};

class Observation {
protected:
  uWS::Loop* loop; // Observable logic are loop-based for simpler logic, I/O operations work in I/O threads
public:
  virtual void close();
};

class ObjectObservation : public Observation {

};

class RangeObservation : public Observation {

};

class CountObservation : public Observation {

};

class Database;


class Store : public std::enable_shared_from_this<Store> {
private:
  std::shared_ptr<Database> database;
  std::vector<std::weak_ptr<Observation>> observations;
  std::string name;
  MDB_env* env;
  MDB_dbi dbi;
  std::mutex stateMutex;
  bool finished;
public:

  Store(std::shared_ptr<Database> databasep, std::string namep, MDB_env* envp)
  : database(databasep), name(namep), env(envp) {

  }

  int create() {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    int ret = mdb_dbi_open(txn, name.c_str(), MDB_CREATE, &dbi);
    return ret;
  }

  bool open() {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    int ret = mdb_dbi_open(txn, name.c_str(), 0, &dbi);
    mdb_txn_commit(txn);
    return ret == 0;
  }

  void close() {
    std::lock_guard lock(stateMutex);
    finished = true;
    for(auto const& weak : observations) {
      std::shared_ptr<Observation> observation = weak.lock();
      if(observation) observation->close();
    }
    mdb_dbi_close(env, dbi);
  }

  void drop() {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    /*int ret = */mdb_drop(txn, dbi, 1);
    mdb_txn_commit(txn);
  }

  void put(std::string_view keyp, std::string_view valuep,
           std::function<void()> onOk) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    std::string value = std::string(valuep);
    taskQueue.enqueue([loop, self, this, key, value, onOk]() {
      if(finished) {
        loop->defer([onOk]() { onOk(); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, 0, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      MDB_val valueVal = { .mv_size = value.size(), .mv_data = (void*)value.data() };
      /*int ret = */mdb_put(txn, dbi, &keyVal, &valueVal, 0);
      mdb_txn_commit(txn);
      loop->defer([onOk]() {
        onOk();
      });
    });
  }
  void del(std::string_view keyp,
           std::function<void()> onOk) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    taskQueue.enqueue([loop, self, this, key, onOk]() {
      if(finished) {
        loop->defer([onOk]() { onOk(); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, 0, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      /*int ret = */mdb_del(txn, dbi, &keyVal, 0);
      mdb_txn_commit(txn);
      loop->defer([onOk]() {
        onOk();
      });
    });
  }
  void get(std::string_view keyp,
           std::function<void(const std::string&)> callback) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    taskQueue.enqueue([loop, self, this, key, callback]() {
      if(finished) {
        loop->defer([callback]() { callback(""); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      MDB_val valueVal;
      int ret = mdb_get(txn, dbi, &keyVal, &valueVal);
      mdb_txn_commit(txn);
      std::string value = ret == MDB_NOTFOUND ? "" : std::string((const char*)valueVal.mv_data, valueVal.mv_size);
      loop->defer([callback, value]() {
        callback(value);
      });
    });
  }
  void getRange(RangeView rangeView,
                std::function<void(const std::string& key, const std::string& value)> onValue,
                std::function<void()> onEnd) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    Range range(rangeView);
    taskQueue.enqueue([loop, self, this, range, onValue, onEnd]() {
      if(finished) {
        loop->defer([onEnd]() { onEnd(); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
      MDB_cursor *cursor;
      mdb_cursor_open(txn, dbi, &cursor);
      //MDB_val keyVal;
      //MDB_val valueVal;

      /*

       const min = range.gt || range.gte
          const max = range.lt || range.lte
          if(range.reverse) {
            if(max) {
              found = cursor.goToRange(max)
              if(!found) found = cursor.goToLast()
            } else {
              found = cursor.goToLast()
            }
            while((!range.limit || keys.length < range.limit) && found !== null) {
              if(range.gt && found <= range.gt) break;
              if(range.gte && found < range.gte) break;
              if((!range.lt || found < range.lt) && (!range.lte || found <= range.lte)) {
                // key in range, skip keys outside range
                keys.push(found)
              }
              found = cursor.goToPrev()
            }
          } else {
            if(min) {
              found = cursor.goToRange(min)
            } else {
              found = cursor.goToFirst()
            }
            while((!range.limit || keys.length < range.limit) && found !== null) {
              if(range.lt && found >= range.lt) break;
              if(range.lte && found > range.lte) break;
              if((!range.gt || found > range.gt) && (!range.gte || found >= range.gte)) {
                // key in range, skip keys outside range
                keys.push(found)
              }
              //console.log("    GO TO NEXT [")
              found = cursor.goToNext()
              //console.log("    ] GO TO NEXT")
            }
          }

       */

      mdb_cursor_close(cursor);
      mdb_txn_abort(txn);
    });
  }

  void getCount(RangeView rangeView,
                std::function<void(int)> onCount) {
    //std::shared_ptr<Store> self = shared_from_this();
    //uWS::Loop* loop = uWS::Loop::get();
    //Range range(rangeView);
/*    taskQueue.enqueue([loop, self, this, range]() {

    });*/
  }

  std::shared_ptr<Observation> observeObject(std::string_view key,
                                             std::function<void (const std::string& value)> onState) {
    return nullptr;
  }

  std::shared_ptr<Observation> observeRange(RangeView rangeView,
                    std::function<void (const std::string& key, const std::string& value)> onResult,
                    std::function<void()> onChanges) {
    return nullptr;
  }

  std::shared_ptr<Observation> observeCount(RangeView rangeView,
                    std::function<void(int)> onCount) {
    return nullptr;
  }

};

class Database : public std::enable_shared_from_this<Database> {
private:
  MDB_env* env;
  std::mutex stateMutex;
  std::map<std::string, std::weak_ptr<Store>> stores;
  bool finished = false;
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
        loop->defer([callback, store](){
          callback(store);
        });
        return;
      }
      store = std::make_shared<Store>(self, storeName, env);
      if(store->open()) {
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

      int ret = store->create();
      if(ret == 0) {
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

std::map<std::string, std::weak_ptr<Database>> databases;
std::mutex databasesMutex;

void getDatabase(std::string name, std::function<void(std::shared_ptr<Database>)> callback) {
  uWS::Loop* loop = uWS::Loop::get();
  taskQueue.enqueue([name, callback, loop]() {
    std::lock_guard<std::mutex> databasesLock(databasesMutex);
    std::shared_ptr<Database> database = nullptr;
    auto it = databases.find(name);
    if(it != databases.end()) {
      database = it->second.lock();
    }
    if(database != nullptr) {
      loop->defer([callback, database](){
        callback(database);
      });
      return;
    }
    std::string path = options::dir + "/" + name;
    if(!std::filesystem::exists(path)) {
      loop->defer([callback, database]() {
        callback(nullptr);
      });
      return;
    }
    database = std::make_shared<Database>(path);
    loop->defer([callback, database]() {
      callback(database);
    });
  });
}
void createDatabase(std::string name, std::string settingsJson,
                    std::function<void()> onOk, std::function<void(std::string)> onError) {
  uWS::Loop* loop = uWS::Loop::get();
  taskQueue.enqueue([name, onOk, onError, loop]() {
    auto it = databases.find(name);
    if(it != databases.end()) {
      loop->defer([onError]() {
        onError("exists");
      });
    }
    std::string path = options::dir + "/" + name;
    if(std::filesystem::exists(path)) {
      loop->defer([onError]() {
        onError("exists");
      });
    }
    std::filesystem::create_directories(path);
    loop->defer([onOk]() {
      onOk();
    });
  });
}
void deleteDatabase(std::string name,
                    std::function<void()> onOk, std::function<void(std::string)> onError) {
  uWS::Loop* loop = uWS::Loop::get();
  taskQueue.enqueue([name, onOk, onError, loop]() {
    std::string path = options::dir + "/" + name;
    if(!std::filesystem::exists(path)) {
      loop->defer([onError]() {
        onError("not_found");
      });
      return;
    }
    auto it = databases.find(name);
    std::shared_ptr<Database> database = nullptr;
    if(it != databases.end()) {
      database = it->second.lock();
    }
    if(database != nullptr) database->close();
    std::filesystem::remove_all(path);
    if(it != databases.end()) {
      databases.erase(it);
    }
    loop->defer([onOk]() {
      onOk();
    });
  });
}

class ClientConnection : public std::enable_shared_from_this<ClientConnection> {
private:
  std::vector<std::shared_ptr<Store>> openStores;
  std::map<int, std::shared_ptr<Observation>> observations;

  enum class OpCode {
    Ping = 0,
    Pong = 1,
    Ok = 2,
    Error = 3,

    CreateDatabase = 10,
    DeleteDatabase = 11,
    CreateStore = 12,
    DeleteStore = 13,
    OpenStore = 14,
    CloseStore = 15,

    Put = 30,
    Delete = 31,
    DeleteRange = 32,

    Get = 40,
    GetRange = 41,
    GetCount = 42,

    Observe = 50,
    ObserveRange = 51,
    ObserveCount = 52,
    Unobserve = 59,

    Result = 80,
    ResultPut = 81,
    ResultCount = 82,
    ResultsChanges = 88,
    ResultsDone = 89
  };

  std::function<void (char*, int)> sendCallback;
  std::function<void ()> closeCallback;
public:

  void handleOpen(std::function<void (char*, int)> sendP, std::function<void ()> closeP) {
    sendCallback = std::move(sendP);
    closeCallback = std::move(closeP);
  }
  void handleClose() {

  }
  void handleMessage(char* buffer, int size) {
    net::PacketBuffer packet(buffer, size);
    fprintf(stderr,"RECEIVED PACKET!\n");
    packet.print();
    uint8_t opCode = packet.readU8();
    std::shared_ptr<ClientConnection> self = shared_from_this();
    std::weak_ptr<ClientConnection> weak = self;
    switch(opCode) {
      case (uint8_t)OpCode::Ping:
        packet.setU8(0, (uint8_t)OpCode::Pong);
        sendCallback(packet.getPointer(0), packet.size());
        break;
      case (uint8_t)OpCode::Pong:
        /// ignore for now
        break;

      case (uint8_t)OpCode::CreateDatabase: {
        int requestId = packet.readU32();
        std::string databaseName = packet.readString(packet.readU8());
        std::string jsonSettings = packet.readString(packet.readU16());
        createDatabase(databaseName, jsonSettings, [requestId, self](){
          self->sendOk(requestId);
        }, [requestId, self](std::string error){
          self->sendError(requestId, error);
        });
      } break;
      case (uint8_t)OpCode::DeleteDatabase: {
        int requestId = packet.readU32();
        std::string databaseName = packet.readString(packet.readU8());
        deleteDatabase(databaseName, [requestId, this](){
          sendOk(requestId);
        }, [requestId, this](std::string error){
          sendError(requestId, error);
        });
      } break;

      case (uint8_t)OpCode::OpenStore: {
        int requestId = packet.readU32();
        int storeId = openStores.size();
        std::string databaseName = packet.readString(packet.readU8());
        std::string storeName = packet.readString(packet.readU8());
        getDatabase(databaseName, [requestId, storeId, self, storeName](
            std::shared_ptr<Database> database) {
          if(!database) {
            self->openStores[storeId] = nullptr;
            self->sendError(requestId, "database_not_found");
            return;
          }
          database->getStore(
            storeName, [requestId, storeId, self](std::shared_ptr<Store> store){
              if(!store) {
                self->sendError(requestId, "not_found");
                return;
              }
              self->openStores[storeId] = store;
              self->sendOk(requestId);
            });
        });

      } break;
      case (uint8_t)OpCode::CloseStore: {
        int requestId = packet.readU32();
        int id = packet.readU32();
        if(id >= openStores.size() || !openStores[id]) {
          sendError(requestId, "not_opened");
          break;
        }
        openStores[id] = nullptr;
        sendOk(requestId);
      } break;
      case (uint8_t)OpCode::CreateStore: {
        int requestId = packet.readU32();
        std::string databaseName = packet.readString(packet.readU8());
        std::string storeName = packet.readString(packet.readU8());
        getDatabase(databaseName, [requestId, self, storeName](std::shared_ptr<Database> database) {
          if (!database) {
            self->sendError(requestId, "database_not_found");
            return;
          }
          database->createStore(storeName, [requestId, self](){
            self->sendOk(requestId);
          }, [requestId, self](std::string error){
            self->sendError(requestId, error);
          });
        });
      } break;
      case (uint8_t)OpCode::DeleteStore: {
        int requestId = packet.readU32();
        std::string databaseName = packet.readString(packet.readU8());
        std::string storeName = packet.readString(packet.readU8());
        getDatabase(databaseName, [requestId, self, storeName](std::shared_ptr<Database> database) {
          if (!database) {
            self->sendError(requestId, "database_not_found");
            return;
          }
          database->deleteStore(storeName, [requestId, self](){
            self->sendOk(requestId);
          }, [requestId, self](std::string error){
            self->sendError(requestId, error);
          });
        });
      } break;

      case (uint8_t)OpCode::Put: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        int keySize = packet.readU16();
        char *keyData = packet.readPointer(keySize);
        std::string_view key(keyData, keySize);
        int valueSize = packet.readU32();
        char *valueData = packet.readPointer(valueSize);
        std::string_view value(valueData, valueSize);
        store->put(key, value, [requestId, self](){
          self->sendOk(requestId);
        });
        sendOk(requestId);
      } break;
      case (uint8_t)OpCode::Delete: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if(storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        int keySize = packet.readU16();
        char* keyData = packet.readPointer(keySize);
        std::string_view key(keyData, keySize);
        store->del(key, [requestId, self](){
          self->sendOk(requestId);
        });
        sendOk(requestId);
      } break;

      case (uint8_t)OpCode::Get: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if(storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        int keySize = packet.readU16();
        char* keyData = packet.readPointer(keySize);
        std::string_view key(keyData, keySize);
        store->get(key, [this, requestId](const std::string& obj) {
          net::PacketBuffer resultPacket(obj.size() + 12);
          resultPacket.writeU8((uint8_t)OpCode::Result);
          resultPacket.writeU32(requestId);
          resultPacket.writeBytes(obj.data(), obj.size());
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        });
      } break;
      case (uint8_t)OpCode::GetRange: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        RangeView range(packet);
        store->getRange(range, [this, requestId](std::string_view key, std::string_view value) {
          net::PacketBuffer resultPacket(key.size() + value.size() + 14);
          resultPacket.writeU8((uint8_t) OpCode::ResultPut);
          resultPacket.writeU32(requestId);
          resultPacket.writeU16(key.size());
          resultPacket.writeBytes(key.data(), key.size());
          resultPacket.writeU32(value.size());
          resultPacket.writeBytes(value.data(), value.size());
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        }, [this, requestId]() {
          net::PacketBuffer resultPacket(10);
          resultPacket.writeU8((uint8_t) OpCode::ResultsDone);
          resultPacket.writeU32(requestId);
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        });
      } break;
      case (uint8_t)OpCode::GetCount: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        RangeView range(packet);
        store->getCount(range, [this, requestId](int count) {
          net::PacketBuffer resultPacket(12);
          resultPacket.writeU8((uint8_t) OpCode::ResultCount);
          resultPacket.writeU32(requestId);
          resultPacket.writeU32(count);
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        });
      } break;

      case (uint8_t)OpCode::Observe: {
        int requestId = packet.readU32();
        if(observations.find(requestId) != observations.end()) {
          sendError(requestId, "exists");
          break;
        }
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        int keySize = packet.readU16();
        char* keyData = packet.readPointer(keySize);
        std::string_view key = std::string_view(keyData, keySize);
        std::shared_ptr<Observation> observation =
          store->observeObject(key, [requestId, this](const std::string& value){
            net::PacketBuffer resultPacket(value.size() + 12);
            resultPacket.writeU8((uint8_t)OpCode::Result);
            resultPacket.writeU32(requestId);
            resultPacket.writeBytes(value.data(), value.size());
            resultPacket.flip();
            sendCallback(resultPacket.getPointer(0), resultPacket.size());
          });
        observations[requestId] = observation;
      } break;
      case (uint8_t)OpCode::ObserveRange: {
        int requestId = packet.readU32();
        if(observations.find(requestId) != observations.end()) {
          sendError(requestId, "exists");
          break;
        }
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        RangeView range(packet);
        std::shared_ptr<Observation> observation =
          store->observeRange(range, [requestId, this](std::string_view key, std::string_view value){
            net::PacketBuffer resultPacket(key.size() + value.size() + 14);
            resultPacket.writeU8((uint8_t) OpCode::ResultPut);
            resultPacket.writeU32(requestId);
            resultPacket.writeU16(key.size());
            resultPacket.writeBytes(key.data(), key.size());
            resultPacket.writeU32(value.size());
            resultPacket.writeBytes(value.data(), value.size());
            resultPacket.flip();
            sendCallback(resultPacket.getPointer(0), resultPacket.size());
          }, [requestId, this](){
            net::PacketBuffer resultPacket(12);
            resultPacket.writeU8((uint8_t)OpCode::ResultsChanges);
            resultPacket.writeU32(requestId);
            resultPacket.flip();
            sendCallback(resultPacket.getPointer(0), resultPacket.size());
          });
        observations[requestId] = observation;
      } break;
      case (uint8_t)OpCode::ObserveCount: {
        int requestId = packet.readU32();
        if(observations.find(requestId) != observations.end()) {
          sendError(requestId, "exists");
          break;
        }
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        RangeView range(packet);
        std::shared_ptr<Observation> observation =
          store->observeCount(range, [requestId, this](int count){
            net::PacketBuffer resultPacket(12);
            resultPacket.writeU8((uint8_t)OpCode::ResultCount);
            resultPacket.writeU32(requestId);
            resultPacket.writeU32(count);
            resultPacket.flip();
            sendCallback(resultPacket.getPointer(0), resultPacket.size());
          });
      } break;

      case (uint8_t)OpCode::Unobserve: {
        int requestId = packet.readU32();
        auto it = observations.find(requestId);
        if(it != observations.end()) {
          sendError(requestId, "not_found");
          break;
        }
        observations.erase(it);
        it->second->close();
      } break;
    }
  }
  
  void sendError(int requestId, std::string error) {
    net::PacketBuffer resultPacket(128);
    resultPacket.writeU8((uint8_t)OpCode::Error);
    resultPacket.writeU32(requestId);
    resultPacket.writeString(error);
    resultPacket.flip();
    fprintf(stderr,"SEND PACKET!\n");
    resultPacket.print();
    sendCallback(resultPacket.getPointer(0), resultPacket.size());
  }
  
  void sendOk(int requestId) {
    net::PacketBuffer resultPacket(10);
    resultPacket.writeU8((uint8_t)OpCode::Ok);
    resultPacket.writeU32(requestId);
    resultPacket.flip();
    fprintf(stderr,"SEND PACKET!\n");
    resultPacket.print();
    sendCallback(resultPacket.getPointer(0), resultPacket.size());
  }
  
};

struct us_listen_socket_t *global_listen_socket;

int main(int argc, char** argv) {
  parseOptions(argc, argv);

  std::filesystem::create_directories(options::dir);

  /*auto env = lmdb::env::create();
  env.set_mapsize(1UL * 1024UL * 1024UL * 1024UL);*/

  auto wsApp = uWS::App({
    .key_file_name = "../misc/key.pem",
    .cert_file_name = "../misc/cert.pem",
    .passphrase = "1234"
  });

  struct PerSocketData {
    std::shared_ptr<ClientConnection> connection;
  };

  for(int i = 0; i < options::threads; i++) {
    std::thread worker(workerThread);
    lmdbThreadPool.push_back(std::move(worker));
  }

  wsApp.ws<PerSocketData>("/*", {
      /* Settings */
      .compression = uWS::DEDICATED_COMPRESSOR_3KB,
      .maxPayloadLength = 16 * 1024 * 1024,
      .idleTimeout = 10,
      .maxBackpressure = 1 * 1024 * 1024,
      /* Handlers */
      .upgrade = nullptr,
      .open = [](auto *ws) {
        PerSocketData* socketData = (PerSocketData*)ws->getUserData();
        socketData->connection = std::make_shared<ClientConnection>();
        socketData->connection->handleOpen([ws](char* data, int size) {
          ws->send(std::string_view(data, size), uWS::OpCode::BINARY);
        }, [ws]() {
          ws->close();
        });

      },
      .message = [](auto *ws, std::string_view message, uWS::OpCode opCode) {
        PerSocketData* socketData = (PerSocketData*)ws->getUserData();
        if(opCode == uWS::OpCode::BINARY) {
          socketData->connection->handleMessage((char*)message.data(), message.size());
        } else {
        }
      },
      .drain = [](auto */*ws*/) {
        /* Check getBufferedAmount here */
      },
      .ping = [](auto */*ws*/) {

      },
      .pong = [](auto */*ws*/) {

      },
      .close = [](auto *ws, int /*code*/, std::string_view /*message*/) {
        PerSocketData* socketData = (PerSocketData*)ws->getUserData();
        socketData->connection->handleClose();
      }
  }).listen(options::port, [](auto *listen_socket) {
    if (listen_socket) {
      global_listen_socket = listen_socket;
      if (listen_socket) {
        std::cout << "Listening on port " << 9001 << std::endl;
      }
    }
  }).run();

  return 0;
}

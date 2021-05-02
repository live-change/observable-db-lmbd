#include <stdio.h>
#include <boost/program_options.hpp>
#include <uWebSockets/App.h>
#include <filesystem>
#include <unordered_set>
#include "PacketBuffer.h"
#include "range.h"
#include "Database.h"

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
    ResultNotFound = 83,
    ResultsChanges = 88,
    ResultsDone = 89
  };

  enum class ResultPutFlags {
    Found = 0x1,
    Last = 0x2,
    First = 0x4
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
    //fprintf(stderr,"RECEIVED PACKET!\n");
    //packet.print();
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
        openStores.push_back(nullptr);
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
        store->put(key, value, [this, requestId](bool found, const std::string& obj) {
          sendResult(requestId, found, obj);
        });
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
        store->del(key, [this, requestId](bool found, const std::string& obj) {
          sendResult(requestId, found, obj);
        });
      } break;
      case (uint8_t)OpCode::DeleteRange: {
        int requestId = packet.readU32();
        int storeId = packet.readU32();
        std::shared_ptr<Store> store = storeId < openStores.size() ? openStores[storeId] : nullptr;
        if (storeId >= openStores.size() || !openStores[storeId]) {
          sendError(requestId, "not_opened");
          break;
        }
        RangeView range(packet);
        store->deleteRange(range, [this, requestId](int count, const std::string& lastKey) {
          net::PacketBuffer resultPacket(12);
          resultPacket.writeU8((uint8_t) OpCode::ResultCount);
          resultPacket.writeU32(requestId);
          resultPacket.writeU32(count);
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        });
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
        store->get(key, [this, requestId](bool found, const std::string& obj) {
          sendResult(requestId, found, obj);
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
        store->getCount(range, [this, requestId](int count, const std::string& lastKey) {
          net::PacketBuffer resultPacket(12 + lastKey.length());
          resultPacket.writeU8((uint8_t) OpCode::ResultCount);
          resultPacket.writeU32(requestId);
          resultPacket.writeU32(count);
          resultPacket.writeString(lastKey);
          resultPacket.flip();
          sendCallback(resultPacket.getPointer(0), resultPacket.size());
        });
      } break;

      case (uint8_t)OpCode::Observe: {
        fprintf(stderr, "RECEIVED OBSERVE\n");
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
        fprintf(stderr, "OBSERVE OBJECT\n");
        std::shared_ptr<Observation> observation =
          store->observeObject(key, [requestId, this](bool found, const std::string& value){
            if(found) {
              net::PacketBuffer resultPacket(value.size() + 12);
              resultPacket.writeU8((uint8_t) OpCode::Result);
              resultPacket.writeU32(requestId);
              resultPacket.writeBytes(value.data(), value.size());
              resultPacket.flip();
              sendCallback(resultPacket.getPointer(0), resultPacket.size());
            } else {
              net::PacketBuffer resultPacket(10);
              resultPacket.writeU8((uint8_t) OpCode::ResultNotFound);
              resultPacket.writeU32(requestId);
              resultPacket.flip();
              sendCallback(resultPacket.getPointer(0), resultPacket.size());
            }
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
          store->observeRange(range, [requestId, this](bool found, bool last, std::string_view key, std::string_view value){
            net::PacketBuffer resultPacket(key.size() + value.size() + 14);
            resultPacket.writeU8((uint8_t) OpCode::ResultPut);
            resultPacket.writeU32(requestId);
            int flags = (found ? (int)ResultPutFlags::Found : 0) | (last ? (int)ResultPutFlags::Last : 0);
            resultPacket.writeU8(flags);
            resultPacket.writeU16(key.size());
            resultPacket.writeBytes(key.data(), key.size());
            if(found) {
              resultPacket.writeU32(value.size());
              resultPacket.writeBytes(value.data(), value.size());
            }
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
        std::shared_ptr<Observation> observation = it->second;
        observations.erase(it);
        observation->close();
      } break;
    }
  }

  void sendResult(int requestId, bool found, const std::string& obj) {
    if(found) {
      net::PacketBuffer resultPacket(obj.size() + 12);
      resultPacket.writeU8((uint8_t) OpCode::Result);
      resultPacket.writeU32(requestId);
      resultPacket.writeBytes(obj.data(), obj.size());
      resultPacket.flip();
      sendCallback(resultPacket.getPointer(0), resultPacket.size());
    } else {
      net::PacketBuffer resultPacket(10);
      resultPacket.writeU8((uint8_t) OpCode::ResultNotFound);
      resultPacket.writeU32(requestId);
      resultPacket.flip();
      sendCallback(resultPacket.getPointer(0), resultPacket.size());
    }
  }
  
  void sendError(int requestId, std::string error) {
    net::PacketBuffer resultPacket(128);
    resultPacket.writeU8((uint8_t)OpCode::Error);
    resultPacket.writeU32(requestId);
    resultPacket.writeString(error);
    resultPacket.flip();
    fprintf(stderr,"SEND ERROR %s!\n", error.c_str());
    sendCallback(resultPacket.getPointer(0), resultPacket.size());
  }
  
  void sendOk(int requestId) {
    net::PacketBuffer resultPacket(10);
    resultPacket.writeU8((uint8_t)OpCode::Ok);
    resultPacket.writeU32(requestId);
    resultPacket.flip();
/*    fprintf(stderr,"SEND PACKET!\n");
    resultPacket.print();*/
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
          if(data[0] == 0x1e) {
            fprintf(stderr, "WTF?!\n");
          }
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

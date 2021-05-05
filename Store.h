//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_STORE_H
#define OBSERVABLE_DB_LMDB_STORE_H

#include <memory>
#include <mutex>
#include <map>
#include <lmdb.h>
#include <uWebSockets/Loop.h>
#include <boost/icl/interval_map.hpp>
#include <iostream>

#include "taskQueue.h"
#include "observation.h"
#include "range.h"
#include "WeakFastSet.h"

class Database;

class Store : public std::enable_shared_from_this<Store> {
private:
  std::shared_ptr<Database> database;

  int lastObservationId = 0;
  using ObjectObservationSet = std::map<int, std::weak_ptr<ObjectObservation>>;
  std::unordered_map<std::string, ObjectObservationSet> objectObservations;

  boost::icl::interval_map<std::string, std::set<int>> rangeObservations;
  WeakFastSet<RangeObservation> allRangeObservations;


  std::string name;
  MDB_env* env;
  MDB_dbi dbi;
  std::mutex stateMutex;
  bool finished = false;
  friend class Database;
public:

  Store(std::shared_ptr<Database> databasep, std::string namep, MDB_env* envp)
      : database(databasep), name(namep), env(envp) {

  }

  ~Store() {
    fprintf(stderr, "STORE DESTROYED! %s\n", name.c_str());
    close();
  }

  int create() {
    std::lock_guard lock(stateMutex);
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    int ret = mdb_dbi_open(txn, name.c_str(), MDB_CREATE, &dbi);
    mdb_txn_commit(txn);
    fprintf(stderr, "CREATE %s RET %d\n", name.c_str(), ret);
    return ret;
  }

  int open() {
    std::lock_guard lock(stateMutex);
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    int ret = mdb_dbi_open(txn, name.c_str(), 0, &dbi);
    mdb_txn_commit(txn);
    fprintf(stderr, "OPEN %s RET %d\n", name.c_str(), ret);
    return ret;
  }

  void close() {
    std::lock_guard lock(stateMutex);
    finished = true;
    for(auto const& [key, observations] : objectObservations) {
      for(auto const& [id, weak] : observations) {
        std::shared_ptr<ObjectObservation> observation = weak.lock();
        if(observation != nullptr) observation->close();
      }
    }
    for(auto const& weak : allRangeObservations) {
      std::shared_ptr<RangeObservation> observation = weak.lock();
      if(observation != nullptr) observation->close();
    }
    mdb_dbi_close(env, dbi);
  }

  void drop() {
    MDB_txn* txn;
    mdb_txn_begin(env, nullptr, 0, &txn);
    /*int ret = */mdb_drop(txn, dbi, 1);
    mdb_txn_commit(txn);
  }

  std::shared_ptr<Observation> observeObject(std::string_view keyp,
                                             std::function<void (bool found, const std::string& value)> onState) {
    std::lock_guard lock(stateMutex);
    int id = ++lastObservationId;
    std::string key(keyp);
    fprintf(stderr, "observe %s\n", key.c_str());
    std::shared_ptr<ObjectObservation> observation =
        std::make_shared<ObjectObservation>(shared_from_this(), key, id, onState);
    auto emplaced = objectObservations.try_emplace(key);
    emplaced.first->second.emplace(id, observation);
    return observation;
  }
  void handleObjectObservationRemoved(const std::string& key, int id) {
    std::lock_guard lock(stateMutex);
    auto it = objectObservations.find(key);
    if(it != objectObservations.end()) {
      it->second.erase(id);
    }
  }

  std::shared_ptr<RangeDataObservation> observeRange(RangeView rangeView,
                                            RangeDataObservation::Callback onResult,
                                            std::function<void()> onChanges) {
    std::lock_guard lock(stateMutex);
    Range range(rangeView);
    int id = allRangeObservations.findEmpty();
    fprintf(stderr, "observe data %d  '%s' - '%s' %d id:%d\n",
            range.flags, range.gt.c_str(), range.lt.c_str(), range.limit, id);
    std::shared_ptr<RangeDataObservation> observation =
        std::make_shared<RangeDataObservation>(shared_from_this(), range, id, onResult, onChanges);
    observation->init();
    allRangeObservations.buffer[id] = observation;
    Range::interval::type interval = range.toInterval();
    std::cout << "interval:" << interval << "\n";
    rangeObservations += std::make_pair(interval, std::set<int>({ id }));
    return observation;
  }

  void handleRangeObservationRemoved(const Range& range, int id) {
    fprintf(stderr,"RANGE OBSERVATION REMOVED %d\n", id);
    std::lock_guard lock(stateMutex);
    Range::interval::type interval = range.toInterval();
    rangeObservations -= std::make_pair(interval, std::set<int>({ id }));
    allRangeObservations.remove(id);
  }

  std::shared_ptr<RangeCountObservation> observeCount(RangeView rangeView,
                                            RangeCountObservation::Callback onCount) {
    std::lock_guard lock(stateMutex);
    Range range(rangeView);
    int id = allRangeObservations.findEmpty();
    fprintf(stderr, "observe count %d  '%s' - '%s' %d id:%d\n",
            range.flags, range.gt.c_str(), range.lt.c_str(), range.limit, id);
    std::shared_ptr<RangeCountObservation> observation =
        std::make_shared<RangeCountObservation>(shared_from_this(), range, id, onCount);
    observation->init();
    fprintf(stderr, "observation initiated!\n");
    allRangeObservations.buffer[id] = observation;
    Range::interval::type interval = range.toInterval();
    std::cout << "interval:" << interval << "\n";
    rangeObservations += std::make_pair(interval, std::set<int>({ id }));
    fprintf(stderr, "interval set!\n");
    return observation;
  }

  void notifyObservers(bool found, bool created, const std::string& key, const std::string& value) {
    fprintf(stderr, "NOTIFY OBSERVERS! %d %d '%s' '%s'\n", found, created, key.c_str(), value.c_str());
    auto objectIt = objectObservations.find(key);
    if(objectIt != objectObservations.end()) {
      for(auto const& [id, weak]  : objectIt->second) {
        std::shared_ptr<ObjectObservation> observation = weak.lock();
        observation->handleUpdate(found, value);
      }
    }
    auto rangeIt = rangeObservations.find(key);
    if (rangeIt != rangeObservations.end()) {
      fprintf(stderr, "found range observations\n");
      for (int id : rangeIt->second) {
        std::shared_ptr<RangeObservation> observation = allRangeObservations.buffer[id].lock();
        observation->handleOperation(found, created, key, value);
      }
    }
  }

  void put(std::string_view keyp, std::string_view valuep,
           std::function<void(bool found, const std::string&)> onResult) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    std::string value = std::string(valuep);
    taskQueue.enqueue([loop, self, this, key, value, onResult]() {
      if(finished) {
        loop->defer([onResult]() { onResult(false, ""); });
        return;
      }
      int ret;
      MDB_txn *txn;
      ret = mdb_txn_begin(env, nullptr, 0, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      MDB_val valueVal = { .mv_size = value.size(), .mv_data = (void*)value.data() };

      MDB_val oldValueVal;
      int getRet = mdb_get(txn, dbi, &keyVal, &oldValueVal);
      ret = mdb_put(txn, dbi, &keyVal, &valueVal, 0);
      fprintf(stderr, "PUT RET %d %zd %zd\n", ret, keyVal.mv_size, valueVal.mv_size);
      mdb_txn_commit(txn);
      fprintf(stderr, "PUT %d %s = %s\n", dbi, key.c_str(), value.c_str());

      notifyObservers(true, getRet == MDB_NOTFOUND, key, value);

      if(getRet == MDB_NOTFOUND) {
        loop->defer([onResult]() {
          onResult(false, "");
        });
      } else {
        std::string oldValue((char*)oldValueVal.mv_data, oldValueVal.mv_size);
        loop->defer([onResult, oldValue{std::move(oldValue)}]() {
          onResult(true, oldValue);
        });
      }
    });
  }
  void del(std::string_view keyp,
           std::function<void(bool found, const std::string&)> onResult) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    taskQueue.enqueue([loop, self, this, key, onResult]() {
      if(finished) {
        loop->defer([onResult]() { onResult(false, ""); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, 0, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      MDB_val valueVal;
      int getRet = mdb_get(txn, dbi, &keyVal, &valueVal);
      /*int ret = */mdb_del(txn, dbi, &keyVal, 0);
      mdb_txn_commit(txn);

      if(getRet != MDB_NOTFOUND) {
        notifyObservers(false, false, key, "");
      }

      if(getRet == MDB_NOTFOUND) {
        loop->defer([onResult]() {
          onResult(false, "");
        });
      } else {
        std::string value((char*)valueVal.mv_data, valueVal.mv_size);
        loop->defer([onResult, value{std::move(value)}]() {
          onResult(true, value);
        });
      }
    });
  }
  void get(std::string_view keyp,
           std::function<void(bool found, const std::string&)> callback) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    std::string key = std::string(keyp);
    taskQueue.enqueue([loop, self, this, key, callback]() {
      if(finished) {
        fprintf(stderr, "GET WHEN FINISHED!!!\n");
        loop->defer([callback]() { callback(false, ""); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
      MDB_val keyVal = { .mv_size = key.size(), .mv_data = (void*)key.data() };
      MDB_val valueVal;
      int ret = mdb_get(txn, dbi, &keyVal, &valueVal);
      fprintf(stderr, "GET RET %d %zd %zd\n", ret, keyVal.mv_size, valueVal.mv_size);
      mdb_txn_abort(txn);
      if(ret == MDB_NOTFOUND) {
        loop->defer([callback]() {
          callback(false, "");
        });
      } else {
        std::string value((const char*)valueVal.mv_data, valueVal.mv_size);
        fprintf(stderr, "GET %d %s = %s\n", dbi, key.c_str(), value.c_str());
        loop->defer([callback, value]() {
          callback(true, value);
        });
      }
    });
  }
  void getRange(RangeView rangeView,
               std::function<void(const std::string& key, const std::string& value)> onValue,
               std::function<void()> onEnd) {
    Range range(rangeView);
    getRange(range, onValue, onEnd);
  }
  void getRange(Range& range,
                std::function<void(const std::string& key, const std::string& value)> onValue,
                std::function<void()> onEnd) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    taskQueue.enqueue([loop, self, this, range, onValue{std::move(onValue)}, onEnd]() {
      fprintf(stderr, "GET RANGE!\n");
      if(finished) {
        fprintf(stderr, "GET RANGE WHEN FINISHED!\n");
        loop->defer([onEnd]() { onEnd(); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
      MDB_cursor *cursor;
      mdb_cursor_open(txn, dbi, &cursor);

      int readCount = 0;
      bool isLimited = range.flags & RangeFlag::Limit;
      fprintf(stderr,"IS LIMITED %d\n", isLimited);
      int ret;
      MDB_val keyVal, valueVal;

      if(range.flags & RangeFlag::Reverse) {
        if(range.flags & (RangeFlag::Lt | RangeFlag::Lte)) {
          keyVal.mv_size = range.lt.size();
          keyVal.mv_data = (void*)range.lt.data();
          ret = mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.lt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          if(ret == MDB_NOTFOUND) {
            ret = mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_LAST);
            fprintf(stderr, "LAST RET %d\n", ret);
            fprintf(stderr, "KEY AFTER LAST %s = %s\n", range.lt.c_str(),
                    std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          }
        } else {
          mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_LAST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Gt) && keyView <= range.gt) break;
          if((range.flags & RangeFlag::Gte) && keyView < range.gt) break;
          if( (!(range.flags & RangeFlag::Lt) || keyView < range.lt)
              && (!(range.flags & RangeFlag::Lte) || keyView <= range.lt) ) {
            onValue(std::string(keyView), std::string((char*)valueVal.mv_data, valueVal.mv_size));
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_PREV);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s = %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "",
                  valueVal.mv_size > 0 ? std::string((const char*)valueVal.mv_data, valueVal.mv_size).c_str() : "");
        }
      } else {
        if(range.flags & (RangeFlag::Gt | RangeFlag::Gte)) {
          keyVal.mv_size = range.gt.size();
          keyVal.mv_data = (void*)range.gt.data();
          ret = mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.gt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
        } else {
          mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_FIRST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          fprintf(stderr, "RC %d < %d\n", readCount, range.limit);
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Lt) && keyView >= range.lt) break;
          if((range.flags & RangeFlag::Lte) && keyView > range.lt) break;
          if( (!(range.flags & RangeFlag::Gt) || keyView > range.gt)
              && (!(range.flags & RangeFlag::Gte) || keyView >= range.gt) ) {
            onValue(std::string(keyView), std::string((char*)valueVal.mv_data, valueVal.mv_size));
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, &valueVal, MDB_NEXT);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s = %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "",
                  valueVal.mv_size > 0 ? std::string((const char*)valueVal.mv_data, valueVal.mv_size).c_str() : "");
        }
      }
      onEnd();

      mdb_cursor_close(cursor);
      mdb_txn_abort(txn);
    });
  }

  void getCount(RangeView rangeView,
                std::function<void(int, const std::string&)> onCount) {
    Range range(rangeView);
    getCount(range, onCount);
  }

  void getCount(Range& range,
                std::function<void(int, const std::string&)> onCount) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    taskQueue.enqueue([loop, self, this, range, onCount{std::move(onCount)}]() {
      fprintf(stderr, "GET RANGE!\n");
      if(finished) {
        fprintf(stderr, "GET Count WHEN FINISHED!\n");
        loop->defer([onCount]() { onCount(0, ""); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, MDB_RDONLY, &txn);
      MDB_cursor *cursor;
      mdb_cursor_open(txn, dbi, &cursor);

      int readCount = 0;
      bool isLimited = range.flags & RangeFlag::Limit;
      int ret;
      MDB_val keyVal;

      if(range.flags & RangeFlag::Reverse) {
        if(range.flags & (RangeFlag::Lt | RangeFlag::Lte)) {
          keyVal.mv_size = range.lt.size();
          keyVal.mv_data = (void*)range.lt.data();
          ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.lt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          if(ret == MDB_NOTFOUND) {
            ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_LAST);
            fprintf(stderr, "LAST RET %d\n", ret);
            fprintf(stderr, "KEY AFTER LAST %s = %s\n", range.lt.c_str(),
                    std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          }
        } else {
          mdb_cursor_get(cursor, &keyVal, nullptr, MDB_LAST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Gt) && keyView <= range.gt) break;
          if((range.flags & RangeFlag::Gte) && keyView < range.gt) break;
          if( (!(range.flags & RangeFlag::Lt) || keyView < range.lt)
              && (!(range.flags & RangeFlag::Lte) || keyView <= range.lt) ) {
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, nullptr, MDB_PREV);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "");
        }
      } else {
        if(range.flags & (RangeFlag::Gt | RangeFlag::Gte)) {
          keyVal.mv_size = range.gt.size();
          keyVal.mv_data = (void*)range.gt.data();
          ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.gt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
        } else {
          mdb_cursor_get(cursor, &keyVal, nullptr, MDB_FIRST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Lt) && keyView >= range.lt) break;
          if((range.flags & RangeFlag::Lte) && keyView > range.lt) break;
          if( (!(range.flags & RangeFlag::Gt) || keyView > range.gt)
              && (!(range.flags & RangeFlag::Gte) || keyView >= range.gt) ) {
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, nullptr, MDB_NEXT);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "");
        }
      }
      fprintf(stderr, "COUNT RESULT %d\n", readCount);
      std::string lastKey = std::string((char*)keyVal.mv_data, keyVal.mv_size);
      loop->defer([onCount, readCount, lastKey{std::move(lastKey)}]() { onCount(readCount, lastKey); });

      mdb_cursor_close(cursor);
      mdb_txn_abort(txn);
    });
  }

  void deleteRange(RangeView rangeView,
                   std::function<void(int, const std::string&)> onCount) {
    std::shared_ptr<Store> self = shared_from_this();
    uWS::Loop* loop = uWS::Loop::get();
    Range range(rangeView);
    taskQueue.enqueue([loop, self, this, range, onCount{std::move(onCount)}]() {
      fprintf(stderr, "GET RANGE!\n");
      if(finished) {
        fprintf(stderr, "GET Count WHEN FINISHED!\n");
        loop->defer([onCount]() { onCount(0, ""); });
        return;
      }
      MDB_txn *txn;
      mdb_txn_begin(env, nullptr, 0, &txn);
      MDB_cursor *cursor;
      mdb_cursor_open(txn, dbi, &cursor);

      int readCount = 0;
      bool isLimited = range.flags & RangeFlag::Limit;
      int ret;
      MDB_val keyVal;

      if(range.flags & RangeFlag::Reverse) {
        if(range.flags & (RangeFlag::Lt | RangeFlag::Lte)) {
          keyVal.mv_size = range.lt.size();
          keyVal.mv_data = (void*)range.lt.data();
          ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.lt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          if(ret == MDB_NOTFOUND) {
            ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_LAST);
            fprintf(stderr, "LAST RET %d\n", ret);
            fprintf(stderr, "KEY AFTER LAST %s = %s\n", range.lt.c_str(),
                    std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
          }
        } else {
          mdb_cursor_get(cursor, &keyVal, nullptr, MDB_LAST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Gt) && keyView <= range.gt) break;
          if((range.flags & RangeFlag::Gte) && keyView < range.gt) break;
          if( (!(range.flags & RangeFlag::Lt) || keyView < range.lt)
              && (!(range.flags & RangeFlag::Lte) || keyView <= range.lt) ) {
            notifyObservers(false, false, std::string(keyView), "");
            mdb_cursor_del(cursor, 0);
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, nullptr, MDB_PREV);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "");
        }
      } else {
        if(range.flags & (RangeFlag::Gt | RangeFlag::Gte)) {
          keyVal.mv_size = range.gt.size();
          keyVal.mv_data = (void*)range.gt.data();
          ret = mdb_cursor_get(cursor, &keyVal, nullptr, MDB_SET_RANGE);
          fprintf(stderr, "SET_RANGE RET %d\n", ret);
          fprintf(stderr, "KEY AFTER SET_RANGE %s = %s\n", range.gt.c_str(),
                  std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str());
        } else {
          mdb_cursor_get(cursor, &keyVal, nullptr, MDB_FIRST);
        }
        while((!isLimited || readCount < range.limit) && ret != MDB_NOTFOUND) {
          std::string_view keyView((char*)keyVal.mv_data, keyVal.mv_size);
          if((range.flags & RangeFlag::Lt) && keyView >= range.lt) break;
          if((range.flags & RangeFlag::Lte) && keyView > range.lt) break;
          if( (!(range.flags & RangeFlag::Gt) || keyView > range.gt)
              && (!(range.flags & RangeFlag::Gte) || keyView >= range.gt) ) {
            notifyObservers(false, false, std::string(keyView), "");
            mdb_cursor_del(cursor, 0);
            readCount++;
          }
          ret =  mdb_cursor_get(cursor, &keyVal, nullptr, MDB_NEXT);
          fprintf(stderr, "RET %d KEY AFTER NEXT %s\n", ret,
                  keyVal.mv_size > 0 ? std::string((const char*)keyVal.mv_data, keyVal.mv_size).c_str() : "");
        }
      }
      fprintf(stderr, "DELETE COUNT %d\n", readCount);
      std::string lastKey = std::string((char*)keyVal.mv_data, keyVal.mv_size);
      loop->defer([onCount, readCount, lastKey{std::move(lastKey)}]() { onCount(readCount, lastKey); });

      mdb_cursor_close(cursor);
      mdb_txn_commit(txn);
    });
  }

};

#endif //OBSERVABLE_DB_LMDB_STORE_H

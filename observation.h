//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_OBSERVATION_H
#define OBSERVABLE_DB_LMDB_OBSERVATION_H

#include <uWebSockets/Loop.h>
#include <memory>
#include "range.h"

class Store;

class Observation {
protected:
  uWS::Loop* loop; // Observable logic are loop-based for simpler logic, I/O operations work in I/O threads
public:
  Observation() {
    loop = uWS::Loop::get();
  }
  virtual void close();
  virtual ~Observation();
};

class ObjectObservation : public Observation, public std::enable_shared_from_this<ObjectObservation> {
public:
  using Callback = std::function<void (bool found, const std::string& value)>;
private:
  std::shared_ptr<Store> store;
  std::string key;
  int id;
  Callback onState;
  bool finished = false;
public:
  ObjectObservation(std::shared_ptr<Store> storep, const std::string& keyp, int idp,
                    Callback onStatep);
  void handleUpdate(bool found, const std::string& value);
  virtual void close();
  virtual ~ObjectObservation();
};

class RangeObservation : public Observation {
protected:
  std::shared_ptr<Store> store;
  Range range;
  int id;
  bool finished = false;
  friend class RangeDataObservation;
public:
  RangeObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp);

  virtual void handleOperation(bool found, bool created, const std::string& key, const std::string& value);

  virtual void close();
  virtual ~RangeObservation();
};

class RangeDataObservation : public RangeObservation, public std::enable_shared_from_this<RangeDataObservation> {
public:
  using Callback = std::function<void (bool found, bool created, bool end, const std::string& key,
      const std::string& value)>;
private:
  Callback onValue;
  std::function<void()> onChanges;
  std::vector<std::string> keys;
  std::vector<std::tuple<bool, bool, std::string, std::string>> waitingOperations;
  bool waitingForRead = true;
public:

  RangeDataObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
                       Callback onValuep, std::function<void()> onChangesp);

  void init();

  virtual void handleOperation(bool found, bool created, const std::string& key, const std::string& value);
  void processOperation(bool found, bool created, const std::string& key, const std::string& value);

  /*virtual void close();
  virtual ~RangeDataObservation();*/
};

class RangeCountObservation : public RangeObservation, public std::enable_shared_from_this<RangeCountObservation> {
public:
  using Callback = std::function<void (int count)>;
private:
  Callback onCount;
  int count = 0;
  int waitingDiff = 0;
  bool needRecount = false;
  bool waitingForRead = false;
  bool lastKeyLimit = false;
  bool firstCount = true;
  std::string lastKey;
public:
  RangeCountObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
      Callback onCountp);

  void init();
  void recount();

  virtual void handleOperation(bool found, bool created, const std::string& key, const std::string& value);
  void processOperation(bool found, bool created, const std::string& key, const std::string& value);

  /*virtual void close();
  virtual ~CountObservation();*/
};


#endif //OBSERVABLE_DB_LMDB_OBSERVATION_H

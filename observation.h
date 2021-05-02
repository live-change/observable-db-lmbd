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
public:
  RangeObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp);

  virtual void handlePut(const std::string& key, const std::string& value);
  virtual void handleDelete(const std::string& key);

  virtual void close();
  virtual ~RangeObservation();
};

class RangeDataObservation : public RangeObservation, public std::enable_shared_from_this<RangeDataObservation> {
public:
  using Callback = std::function<void (bool found, bool end, const std::string& key, const std::string& value)>;
private:
  Callback onValue;
  std::function<void()> onChanges;
  std::vector<std::string> keys;
  bool changesMode = false;
public:

  RangeDataObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
                       Callback onValuep, std::function<void()> onChangesp);

  virtual void handlePut(const std::string& key, const std::string& value);
  virtual void handleDelete(const std::string& key);

  /*virtual void close();
  virtual ~RangeDataObservation();*/
};

class CountObservation : public RangeObservation, public std::enable_shared_from_this<CountObservation> {
public:
  using Callback = std::function<void (int count)>();
private:
  Callback onState;
  int count;
public:
  CountObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
      Callback onStatep);

  virtual void handlePut(const std::string& key, const std::string& value);
  virtual void handleDelete(const std::string& key);

  /*virtual void close();
  virtual ~CountObservation();*/
};


#endif //OBSERVABLE_DB_LMDB_OBSERVATION_H

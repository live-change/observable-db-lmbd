//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_OBSERVATION_H
#define OBSERVABLE_DB_LMDB_OBSERVATION_H

#include <uWebSockets/Loop.h>
#include <memory>

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
private:
  std::shared_ptr<Store> store;
  std::string key;
  int id;
  std::function<void (bool found, const std::string& value)> onState;
  bool finished = false;
public:
  ObjectObservation(std::shared_ptr<Store> storep, const std::string& keyp, int idp,
                    std::function<void (bool found, const std::string& value)> onStatep);
  void handleUpdate(bool found, const std::string& value);
  virtual void close();
  virtual ~ObjectObservation();
};

class RangeObservation : public Observation {

};

class RangeDataObservation : public RangeObservation {

};

class CountObservation : public RangeObservation {

};


#endif //OBSERVABLE_DB_LMDB_OBSERVATION_H

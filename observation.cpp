//
// Created by m8 on 4/29/21.
//

#include "observation.h"
#include "Store.h"

void Observation::close() {}
Observation::~Observation() {}

ObjectObservation::ObjectObservation(std::shared_ptr<Store> storep, const std::string& keyp, int idp,
    std::function<void (bool found, const std::string& value)> onStatep)
    : Observation(), store(storep), key(keyp), id(idp), onState(onStatep) {
  store->get(std::string_view(key), onState);
}

void ObjectObservation::handleUpdate(bool found, const std::string& value) {
  if(finished) return;
  auto self = shared_from_this();
  loop->defer([self, found, value]() {
    self->onState(found, value);
  });
}

void ObjectObservation::close() {
  finished = true;
}

ObjectObservation::~ObjectObservation() {
  store->handleObjectObservationRemoved(key, id);
}

RangeObservation::RangeObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp)
  : store(storep), range(rangep), id(idp) {}
void RangeObservation::handlePut(const std::string& key, const std::string& value) {}
void RangeObservation::handleDelete(const std::string& key) {}
void RangeObservation::close() {
  finished = true;
}
RangeObservation::~RangeObservation() {
  store->handleRangeObservationRemoved(range, id);
}

RangeDataObservation::RangeDataObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
                                           Callback onValuep, std::function<void()> onChangesp)
                                           : RangeObservation(storep, rangep, idp),
                                           onValue(onValuep), onChanges(onChangesp) {
  std::shared_ptr<RangeDataObservation> self = shared_from_this();
  store->getRange(range, [self](const std::string& key, const std::string& value) {
    if(self->finished) return;
    if(!self->changesMode) {
      self->onValue(true, true, key, value);
      self->keys.push_back(key);
      return;
    }
    /// TODO: change logic
  }, [self]() {
    self->changesMode = true;
  });
}

void RangeDataObservation::handlePut(const std::string& key, const std::string& value) {

}
void RangeDataObservation::handleDelete(const std::string& key){

}

CountObservation::CountObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp, Callback onStatep)
 : RangeObservation(storep, rangep, idp) {

}

void CountObservation::handlePut(const std::string& key, const std::string& value) {

}
void CountObservation::handleDelete(const std::string& key){

}


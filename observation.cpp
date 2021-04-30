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

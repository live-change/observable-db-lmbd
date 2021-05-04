//
// Created by m8 on 4/29/21.
//

#include "observation.h"
#include "Store.h"

#include <algorithm>

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
void RangeObservation::close() {
  finished = true;
}
void RangeObservation::handleOperation(bool found, bool created, const std::string& key, const std::string& value) {}
RangeObservation::~RangeObservation() {
  store->handleRangeObservationRemoved(range, id);
}

RangeDataObservation::RangeDataObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
                                           Callback onValuep, std::function<void()> onChangesp)
                                           : RangeObservation(storep, rangep, idp),
                                           onValue(onValuep), onChanges(onChangesp) {
}

void RangeDataObservation::init() {
  if(range.flags & RangeFlag::Limit) keys.reserve(range.limit+1);
  waitingForRead = true;
  std::shared_ptr<RangeDataObservation> self = shared_from_this();
  store->getRange(range, [self](const std::string& key, const std::string& value) {
    if(self->finished) return;
    self->onValue(true, true, true, key, value);
    self->keys.push_back(key);
    return;
  }, [self]() {
    if(self->finished) return;
    self->waitingForRead = false;
    self->onChanges();
    for(auto const& [ found, created, key, value ] : self->waitingOperations) {
      self->processOperation(found, created, key, value);
      if(self->waitingForRead) break;
    }
  });
}

void RangeDataObservation::handleOperation(bool found, bool created, const std::string& key, const std::string& value) {
  std::shared_ptr<RangeDataObservation> self = shared_from_this();
  loop->defer([self, found, created, key{std::move(key)}, value{std::move(value)}](){
    if(self->waitingForRead) {
      self->waitingOperations.push_back(std::make_tuple(found, created, key, value));
      return;
    } else {
      self->processOperation(found, created, key, value);
    }
  });
}

void RangeDataObservation::processOperation(bool found, bool created,
                                            const std::string& key, const std::string& value) {
  if(finished) return;
  if((range.flags & RangeFlag::Gt) && !(key > range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lt) && !(key < range.lt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Gte) && !(key >= range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lte) && !(key <= range.lt)) throw std::runtime_error("key not in range");
  if(range.flags & RangeFlag::Limit) { // complex logic with limits
    if(found) { // add to limit - may overflow
      if (created) { // new object may overflow
        fprintf(stderr, "add to limited\n");
        bool end = ((range.flags & RangeFlag::Reverse) ? keys.back() > key : keys.back() < key);
        fprintf(stderr, "KS %zd LT %d E %d\n", keys.size(), range.limit, end);
        if (keys.size() == range.limit && end)
          return; // insert over limit - ignore
        auto it = keys.end();
        if (!end) {
          it = keys.begin();
          for (; it != keys.end(); it++) {
            if ((range.flags & RangeFlag::Reverse) ? *it < key : *it > key) {
              break;
            }
          }
        }
        if (keys.size() == range.limit) { // insert created overflow
          fprintf(stderr, "overflow!\n");
          onValue(false, false, false, keys.back(), "");
          onValue(true, true, false, key, value);
        } else {
          onValue(true, true, end, key, value);
        }
        if (end) {
          keys.push_back(key);
        } else {
          keys.insert(it, key);
          if(keys.size() > range.limit) {
            fprintf(stderr, "RESIZE KEYS %zd -> %d\n", keys.size(), range.limit);
            keys.resize(range.limit);
          }
        }
      } else { // object update may not overflow - simple case
        onValue(true, false, false, key, value);
      }
    } else {
      fprintf(stderr, "remove from limited\n");
      bool exists = false;
      auto it = keys.begin();
      for (; it != keys.end(); it++) {
        if(*it == key) {
          exists = true;
          break;
        }
        if ((range.flags & RangeFlag::Reverse) ? *it < key : *it > key) {
          break;
        }
      }
      if(exists) {
        onValue(false, false, false, key, "");
        keys.erase(it);
        waitingForRead = true;
        std::shared_ptr<RangeDataObservation> self = shared_from_this();
        Range refillRange = range;
        if(refillRange.flags & RangeFlag::Reverse) {
          refillRange.flags &= ~RangeFlag::Lte;
          refillRange.flags |= RangeFlag::Lt;
          refillRange.lt = keys.back();
          refillRange.limit = range.limit - keys.size();
        } else {
          fprintf(stderr, "REFILL FLAGS %d\n", refillRange.flags);
          refillRange.flags &= ~RangeFlag::Gte;
          fprintf(stderr, "REFILL FLAGS %d\n", refillRange.flags);
          refillRange.flags |= RangeFlag::Gt;
          fprintf(stderr, "REFILL FLAGS %d\n", refillRange.flags);
          refillRange.gt = keys.back();
          refillRange.limit = range.limit - keys.size();
          fprintf(stderr, "REFILL RANGE gt:%s %s:%s limit:%d flags:%d\n", refillRange.gt.c_str(),
                  refillRange.flags & RangeFlag::Lt ? "lt" : (refillRange.flags & RangeFlag::Lte ? "lte" : ""),
                  refillRange.lt.c_str(),
                  refillRange.limit, refillRange.flags);
        }
        store->getRange(refillRange, [self](const std::string& key, const std::string& value) {
          if(self->finished) return;
          self->onValue(true, true, true, key, value);
          self->keys.push_back(key);
          return;
        }, [self]() {
          if(self->finished) return;
          self->waitingForRead = false;
          for(auto const& [ found, created, key, value ] : self->waitingOperations) {
            self->processOperation(found, created, key, value);
            if(self->waitingForRead) break;
          }
        });
      }
    }
  } else { // simple logic without limits
    onValue(found, created, false, key, value);
  }
}

CountObservation::CountObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp, Callback onStatep)
 : RangeObservation(storep, rangep, idp) {

}
void CountObservation::handleOperation(bool found, bool created, const std::string& key, const std::string& value) {

}
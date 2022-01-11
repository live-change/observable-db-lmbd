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
  : Observation(), store(storep), range(rangep), id(idp) {}
void RangeObservation::close() {
  finished = true;
  store->handleRangeObservationRemoved(range, id);
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
    db_log("RANGE READ DONE!");
    if(self->finished) return;
    db_log("RANGE READ DONE!!");
    self->waitingForRead = false;
    self->onChanges();
    std::vector<std::tuple<bool, bool, std::string, std::string>> ops = self->waitingOperations;
    self->waitingOperations.clear();
    for(auto const& [ found, created, key, value ] : ops) {
      self->processOperation(found, created, key, value);
      if(self->waitingForRead) break;
    }
/*    if((self->range.flags & RangeFlag::Gt) && self->store->name == "500004103500000000.data") {
      fprintf(stderr, "INIT Triggers range %s keys %zd\n", self->range.gt.c_str(), self->keys.size());
    }*/
  });
}

void RangeDataObservation::handleOperation(bool found, bool created, const std::string& keyp, const std::string& valuep) {
  db_log("DATA %d HANDLE OPERATION %d %d %s", id, found, created, keyp.c_str());
  std::shared_ptr<RangeDataObservation> self = shared_from_this();
  std::string key = keyp;
  std::string value = valuep;
  loop->defer([self, found, created, key, value](){
    if(self->waitingForRead) {
      self->waitingOperations.push_back(std::make_tuple(found, created, key, value));
      return;
    } else {
      self->processOperation(found, created, key, value);
/*      if((self->range.flags & RangeFlag::Gt) && self->store->name == "500004103500000000.data") {
        fprintf(stderr, "AFTER OP Triggers range %s keys %zd\n", self->range.gt.c_str(), self->keys.size());
      }*/
    }
  });
}

void RangeDataObservation::processOperation(bool found, bool created,
                                            const std::string& key, const std::string& value) {
  db_log("DATA %d PROCESS OPERATION %d %d %s", id, found, created, key.c_str());
  if(finished) return;
  if((range.flags & RangeFlag::Gt) && !(key > range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lt) && !(key < range.lt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Gte) && !(key >= range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lte) && !(key <= range.lt)) throw std::runtime_error("key not in range");
  if(range.flags & RangeFlag::Limit) { // complex logic with limits
    db_log("DATA %d PROCESS LIMITED", id);
    if(found) { // add to limit - may overflow
      if (created) { // new object may overflow
        db_log("add to limited");
        bool end = (keys.size() == 0) || ((range.flags & RangeFlag::Reverse) ? keys.back() > key : keys.back() < key);
        db_log("KS %zd LT %d E %d", keys.size(), range.limit, end);
        if (keys.size() == range.limit && end) {
          for(unsigned int i = 0; i < keys.size(); i++) {
            db_log("KEY %d : %s", i, keys[i].c_str());
          }
          return; // insert over limit - ignore
        }
        auto it = keys.end();
        if (!end) {
          it = keys.begin();
          for (; it != keys.end(); it++) {
            if ((range.flags & RangeFlag::Reverse) ? *it <= key : *it >= key) {
              break;
            }
          }
          if(*it == key) { // key update
            onValue(true, false, false, key, value);
            return;
          }
        }
        if (keys.size() == range.limit) { // insert created overflow
          db_log("overflow!");
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
            db_log("RESIZE KEYS %zd -> %d", keys.size(), range.limit);
            keys.resize(range.limit);
          }
        }
      } else { // object update may not overflow - simple case
        onValue(true, false, false, key, value);
      }
    } else {
      db_log("remove from limited");
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
        bool needRefill = keys.size() == range.limit;
        onValue(false, false, false, key, "");
        keys.erase(it);
        /*if(range.gt == "accessControl_") {
          fprintf(stderr, "KEYS AFTER ERASE %s = %zd   NEED REFILL %d\n", key.c_str(), keys.size(), needRefill);
        }*/
        if(!needRefill) return;
        waitingForRead = true;
        std::shared_ptr<RangeDataObservation> self = shared_from_this();
        Range refillRange = range;
        if(keys.size() > 0) { // else reload all
          if (refillRange.flags & RangeFlag::Reverse) {
            refillRange.flags &= ~RangeFlag::Lte;
            refillRange.flags |= RangeFlag::Lt;
            refillRange.lt = keys.back();
            refillRange.limit = range.limit - keys.size();
          } else {
            db_log("REFILL FLAGS %d", refillRange.flags);
            refillRange.flags &= ~RangeFlag::Gte;
            db_log("REFILL FLAGS %d", refillRange.flags);
            refillRange.flags |= RangeFlag::Gt;
            db_log("REFILL FLAGS %d", refillRange.flags);
            refillRange.gt = keys.back();
            refillRange.limit = range.limit - keys.size();
            db_log("REFILL RANGE gt:%s %s:%s limit:%d flags:%d", refillRange.gt.c_str(),
                    refillRange.flags & RangeFlag::Lt ? "lt" : (refillRange.flags & RangeFlag::Lte ? "lte" : ""),
                    refillRange.lt.c_str(),
                    refillRange.limit, refillRange.flags);
          }
        }
        store->getRange(refillRange, [self](const std::string& key, const std::string& value) {
          if(self->finished) return;
          self->onValue(true, true, true, key, value);
          self->keys.push_back(key);
          return;
        }, [self]() {
          if(self->finished) return;
          /*if((self->range.flags & RangeFlag::Gt) && self->store->name == "500004103500000000.data") {
            fprintf(stderr, "AFTER REFILL Triggers range %s keys %zd\n", self->range.gt.c_str(), self->keys.size());
          }*/
          self->waitingForRead = false;
          std::vector<std::tuple<bool, bool, std::string, std::string>> ops = self->waitingOperations;
          self->waitingOperations.clear();
          for(auto it = ops.begin(); it!=ops.end(); ++it) {
            auto const& [ found, created, key, value ] = *it;
            self->processOperation(found, created, key, value);
            if(self->waitingForRead) { // Reinsert ops that are not finished
              fprintf(stderr, "REFILL STARTED WHEN PROCESSING QUEUED CHANGED!!!\n");
              self->waitingOperations.insert(self->waitingOperations.begin(), it, ops.end());
              break;
            }
          }
/*          if((self->range.flags & RangeFlag::Gt) && self->store->name == "500004103500000000.data") {
            fprintf(stderr, "AFTER REFILL OPS Triggers range %s keys %zd\n", self->range.gt.c_str(), self->keys.size());
          }*/
        });
      }
    }
  } else { // simple logic without limits
    onValue(found, created, false, key, value);
  }
}

RangeCountObservation::RangeCountObservation(std::shared_ptr<Store> storep, const Range& rangep, int idp,
                                             Callback onCountp)
 : RangeObservation(storep, rangep, idp), onCount(onCountp) {
}
void RangeCountObservation::init() {
  recount();
}
void RangeCountObservation::recount() {
  waitingForRead = true;
  needRecount = false;
  std::shared_ptr<RangeCountObservation> self = shared_from_this();
  store->getCount(range, [self](unsigned int cnt, const std::string& last) {
    if(self->finished) return;
    db_log("GET COUNT RESULT %d FIRST COUNT %d", cnt, self->firstCount);
    if((self->range.flags & RangeFlag::Limit) && cnt == self->range.limit) {
      self->lastKey = last;
      self->lastKeyLimit = true;
    }
    if(self->needRecount) {
      db_log("NEED RECOUNT!");
      if(self->firstCount || cnt != self->count) {
        self->firstCount = false;
        self->count = cnt;
        self->onCount(self->count);
      }
      self->recount();
    } else {
      self->waitingForRead = false;
      int newCount = cnt + self->waitingDiff;
      self->waitingDiff = 0;
      if((self->range.flags & RangeFlag::Limit) && self->count > self->range.limit) {
        self->count = self->range.limit;
      }
      if(self->firstCount || newCount != self->count) {
        self->firstCount = false;
        self->count = newCount;
        self->onCount(self->count);
      }
    }
  });
}

void RangeCountObservation::handleOperation(bool found, bool created, const std::string& key, const std::string& value) {
  db_log("COUNT HANDLE OPERATION %d %d %s", found, created, key.c_str());
  std::shared_ptr<RangeCountObservation> self = shared_from_this();
  loop->defer([self, found, created, key{std::move(key)}, value{std::move(value)}](){
    self->processOperation(found, created, key, value);
  });
}
void RangeCountObservation::processOperation(bool found, bool created, const std::string& key, const std::string& value) {
  db_log("COUNT PROCESS OPERATION %d %d %s", found, created, key.c_str());
  if(finished) return;
  if(found && !created) return; // ignore value updates
  if((range.flags & RangeFlag::Gt) && !(key > range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lt) && !(key < range.lt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Gte) && !(key >= range.gt)) throw std::runtime_error("key not in range");
  if((range.flags & RangeFlag::Lte) && !(key <= range.lt)) throw std::runtime_error("key not in range");
  if(range.flags & RangeFlag::Limit) {
    if(lastKeyLimit && (
        ((range.flags & RangeFlag::Reverse) && key<lastKey)
        || (!(range.flags & RangeFlag::Reverse) && key>lastKey)
        )) {
      return; /// ignore keys outside limited range
    }
    if(waitingForRead) {
      if (found) {
        waitingDiff ++;
      } else {
        needRecount = true;
      }
    } else {
      if(found) {
        if(count < range.limit) {
          count ++;
          onCount(count);
        }
      } else {
        if(count < range.limit) {
          count --;
          onCount(count);
        } else {
          recount();
        }
      }
    }
  } else {
    if(waitingForRead) {
      if(found) {
        waitingDiff ++;
      } else {
        waitingDiff --;
      }
    } else {
      if(found) {
        if(created) {
          count ++;
          onCount(count);
        }
      } else {
        count --;
        onCount(count);
      }
    }
  }
}
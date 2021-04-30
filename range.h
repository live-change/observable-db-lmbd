//
// Created by m8 on 4/29/21.
//

#ifndef OBSERVABLE_DB_LMDB_RANGE_H
#define OBSERVABLE_DB_LMDB_RANGE_H

#include "PacketBuffer.h"

namespace RangeFlag {
  static const uint8_t Gt = 0x01;
  static const uint8_t Gte = 0x02;
  static const uint8_t Lt = 0x04;
  static const uint8_t Lte = 0x08;
  static const uint8_t Reverse = 0x10;
  static const uint8_t Limit = 0x20;
}

struct RangeView {
  uint8_t flags = 0;
  std::string_view gt;
  std::string_view lt;
  unsigned int limit = 0;
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
    if(flags & (RangeFlag::Limit)) {
      limit = packet.readU32();
    }
  }
};

struct Range {
  uint8_t flags;
  std::string gt;
  std::string lt;
  unsigned int limit;
  Range(RangeView rv) {
    flags = rv.flags;
    gt = rv.gt;
    lt = rv.lt;
    limit = rv.limit;
  }
};

#endif //OBSERVABLE_DB_LMDB_RANGE_H

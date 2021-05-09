//
// Created by Michał Łaszczewski on 06/10/16.
//

#include "PacketBuffer.h"
#include <cstdlib>
#include <cstring>
#include <stdexcept>

#include "log.h"

// TODO: determine endianess in cmake
#define REVERSE_ENDIAN 1

#define PACKET_DEBUG 1

namespace net {

  int buffersCount = 0;

  void PacketBuffer::resizeBuffer(unsigned int bytes) {
    if(external) throw new std::runtime_error("impossible to resize external buffer");
    char* nbuffer = (char*)malloc(bytes);
    unsigned int cpsize = bytes;
    if(bufferSize < bytes) cpsize = bufferSize;
    memset(nbuffer,0,bytes);
    memcpy(nbuffer, buffer, cpsize);
    free(buffer);
    buffer = nbuffer;
    //buffer = (char *) realloc(buffer, bytes);
    bufferSize = bytes;
  }

  void PacketBuffer::reset() {
    byteSize = 0;
    writePosition = 0;
    readPosition = 0;
  }

  void PacketBuffer::flip() {
    readPosition = 0;
    byteSize = writePosition;
  }

  void PacketBuffer::reserve(unsigned int bytes) {
    if (bytes + writePosition >= bufferSize) {
      resizeBuffer(writePosition + bytes);
    }
  }

  void PacketBuffer::reserveMore(unsigned int bytes) {
/*#ifdef EMSCRIPTEN
    if(bytes + writePosition >= 152631) EM_ASM_({ console.trace("RESERVE",$0,$1) },writePosition,bytes);
#endif*/
    if (bytes + writePosition >= bufferSize) {
      resizeBuffer(writePosition + bytes + 2048);
    }
  }

  unsigned int PacketBuffer::tellg() {
    return readPosition;
  }

  unsigned int PacketBuffer::tellp() {
    return writePosition;
  }

  void PacketBuffer::seekg(unsigned int p) {
    readPosition = p;
  }

  void PacketBuffer::seekp(unsigned int p) {
    writePosition = p;
  }

  void PacketBuffer::setU8(unsigned int p, unsigned char v) {
    char *b = (char *) (&v);
    buffer[p] = b[0];
  }

  void PacketBuffer::setS8(unsigned int p, signed char v) {
    char *b = (char *) (&v);
    buffer[p] = b[0];
  }

  void PacketBuffer::setU16(unsigned int p, unsigned short v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p] = b[1];
    buffer[p + 1] = b[0];
#else
    buffer[p] = b[0];
    buffer[p+1] = b[1];
#endif
  }

  void PacketBuffer::setS16(unsigned int p, signed short v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p] = b[1];
    buffer[p + 1] = b[0];
#else
    buffer[p] = b[0];
    buffer[p+1] = b[1];
#endif
  }

  void PacketBuffer::setU32(unsigned int p, unsigned int v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p] = b[3];
    buffer[p + 1] = b[2];
    buffer[p + 2] = b[1];
    buffer[p + 3] = b[0];
#else
    buffer[p]   = b[0];
    buffer[p+1] = b[1];
    buffer[p+2] = b[2];
    buffer[p+3] = b[3];
#endif
  }

  void PacketBuffer::setS32(unsigned int p, signed int v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p] = b[3];
    buffer[p + 1] = b[2];
    buffer[p + 2] = b[1];
    buffer[p + 3] = b[0];
#else
    buffer[p]   = b[0];
    buffer[p+1] = b[1];
    buffer[p+2] = b[2];
    buffer[p+3] = b[3];
#endif
  }

  void PacketBuffer::setU64(unsigned int p, std::uint64_t v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p]     = b[7];
    buffer[p + 1] = b[6];
    buffer[p + 2] = b[5];
    buffer[p + 3] = b[4];
    buffer[p + 4] = b[3];
    buffer[p + 5] = b[2];
    buffer[p + 6] = b[1];
    buffer[p + 7] = b[0];
#else
    buffer[p]     = b[0];
    buffer[p + 1] = b[1];
    buffer[p + 2] = b[2];
    buffer[p + 3] = b[3];
    buffer[p + 4] = b[4];
    buffer[p + 5] = b[5];
    buffer[p + 6] = b[6];
    buffer[p + 7] = b[7];
#endif
  }
  void PacketBuffer::setS64(unsigned int p, std::int64_t v) {
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    buffer[p]     = b[7];
    buffer[p + 1] = b[6];
    buffer[p + 2] = b[5];
    buffer[p + 3] = b[4];
    buffer[p + 4] = b[3];
    buffer[p + 5] = b[2];
    buffer[p + 6] = b[1];
    buffer[p + 7] = b[0];
#else
    buffer[p]     = b[0];
    buffer[p + 1] = b[1];
    buffer[p + 2] = b[2];
    buffer[p + 3] = b[3];
    buffer[p + 4] = b[4];
    buffer[p + 5] = b[5];
    buffer[p + 6] = b[6];
    buffer[p + 7] = b[7];
#endif
  }

  unsigned char PacketBuffer::getU8(unsigned int p) {
    unsigned char v;
    char *b = (char *) (&v);
    b[0] = buffer[p];
    return v;
  }

  signed char PacketBuffer::getS8(unsigned int p) {
    signed char v;
    char *b = (char *) (&v);
    b[0] = buffer[p];
    return v;
  }

  unsigned short PacketBuffer::getU16(unsigned int p) {
    unsigned short v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[1] = buffer[p];
    b[0] = buffer[p + 1];
#else
    b[0] = buffer[p];
    b[1] = buffer[p+1]
#endif
    return v;
  }

  signed short PacketBuffer::getS16(unsigned int p) {
    signed short v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[1] = buffer[p];
    b[0] = buffer[p + 1];
#else
    b[0] = buffer[p];
    b[1] = buffer[p+1]
#endif
    return v;
  }

  unsigned int PacketBuffer::getU32(unsigned int p) {
    unsigned int v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[3] = buffer[p];
    b[2] = buffer[p + 1];
    b[1] = buffer[p + 2];
    b[0] = buffer[p + 3];
#else
    b[0] = buffer[p];
    b[1] = buffer[p+1];
    b[2] = buffer[p+2];
    b[3] = buffer[p+3];
#endif
    return v;
  }

  signed int PacketBuffer::getS32(unsigned int p) {
    signed int v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[3] = buffer[p];
    b[2] = buffer[p + 1];
    b[1] = buffer[p + 2];
    b[0] = buffer[p + 3];
#else
    b[0] = buffer[p];
    b[1] = buffer[p+1];
    b[2] = buffer[p+2];
    b[3] = buffer[p+3];
#endif
    return v;
  }

  std::uint64_t PacketBuffer::getU64(unsigned int p) {
    std::uint64_t v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[7] = buffer[p];
    b[6] = buffer[p + 1];
    b[5] = buffer[p + 2];
    b[4] = buffer[p + 3];
    b[3] = buffer[p + 4];
    b[2] = buffer[p + 5];
    b[1] = buffer[p + 6];
    b[0] = buffer[p + 7];
#else
    b[0] = buffer[p];
    b[1] = buffer[p + 1];
    b[2] = buffer[p + 2];
    b[3] = buffer[p + 3];
    b[4] = buffer[p + 4];
    b[5] = buffer[p + 5];
    b[6] = buffer[p + 6];
    b[7] = buffer[p + 7];
#endif
    return v;
  }

  std::int64_t PacketBuffer::getS64(unsigned int p) {
    std::int64_t v;
    char *b = (char *) (&v);
#ifdef REVERSE_ENDIAN
    b[7] = buffer[p];
    b[6] = buffer[p + 1];
    b[5] = buffer[p + 2];
    b[4] = buffer[p + 3];
    b[3] = buffer[p + 4];
    b[2] = buffer[p + 5];
    b[1] = buffer[p + 6];
    b[0] = buffer[p + 7];
#else
    b[0] = buffer[p];
    b[1] = buffer[p + 1];
    b[2] = buffer[p + 2];
    b[3] = buffer[p + 3];
    b[4] = buffer[p + 4];
    b[5] = buffer[p + 5];
    b[6] = buffer[p + 6];
    b[7] = buffer[p + 7];
#endif
    return v;
  }

  void PacketBuffer::writeU8(unsigned char v) {
    reserveMore(1);
    setU8(writePosition, v);
    writePosition += 1;
  }

  void PacketBuffer::writeS8(signed char v) {
    reserveMore(1);
    setS8(writePosition, v);
    writePosition += 1;
  }

  void PacketBuffer::writeU16(unsigned short v) {
    reserveMore(2);
    setU16(writePosition, v);
    writePosition += 2;
  }

  void PacketBuffer::writeS16(signed short v) {
    reserveMore(2);
    setS16(writePosition, v);
    writePosition += 2;
  }

  void PacketBuffer::writeU32(unsigned int v) {
    reserveMore(4);
    setU32(writePosition, v);
    writePosition += 4;
  }

  void PacketBuffer::writeS32(signed int v) {
    reserveMore(4);
    setS32(writePosition, v);
    writePosition += 4;
  }

  void PacketBuffer::writeU64(std::uint64_t v) {
    reserveMore(8);
    setU64(writePosition,v);
    writePosition+=8;
  }
  void PacketBuffer::writeS64(std::int64_t v) {
    reserveMore(8);
    setS64(writePosition,v);
    writePosition+=8;
  }

  unsigned char PacketBuffer::readU8() {
    if (readPosition + 1 > byteSize) throw BufferUnderflowException();
    auto v = getU8(readPosition);
    readPosition += 1;
    return v;
  }

  signed char PacketBuffer::readS8() {
    if (readPosition + 1 > byteSize) throw BufferUnderflowException();
    auto v = getS8(readPosition);
    readPosition += 1;
    return v;
  }

  unsigned short PacketBuffer::readU16() {
    if (readPosition + 2 > byteSize) throw BufferUnderflowException();
    auto v = getU16(readPosition);
    readPosition += 2;
    return v;
  }

  signed short PacketBuffer::readS16() {
    if (readPosition + 2 > byteSize) throw BufferUnderflowException();
    auto v = getS16(readPosition);
    readPosition += 2;
    return v;
  }

  unsigned int PacketBuffer::readU32() {
    if (readPosition + 4 > byteSize) throw BufferUnderflowException();
    auto v = getU32(readPosition);
    readPosition += 4;
    return v;
  }

  signed int PacketBuffer::readS32() {
    if (readPosition + 4 > byteSize) throw BufferUnderflowException();
    auto v = getS32(readPosition);
    readPosition += 4;
    return v;
  }

  std::uint64_t PacketBuffer::readU64() {
    if(readPosition+8 > byteSize) throw BufferUnderflowException();
    auto v = getU64(readPosition);
    readPosition += 8;
    return v;
  }
  std::int64_t PacketBuffer::readS64() {
    if(readPosition+8 > byteSize) throw BufferUnderflowException();
    auto v = getS64(readPosition);
    readPosition += 8;
    return v;
  }

  void PacketBuffer::writeUVL(unsigned int number) {
#ifdef PACKET_DEBUG
    return writeU32(number);
#endif

    if (number < 0x80) {
      unsigned char b[1] = {(unsigned char) (number & 0x7F)};
      writeBytes((const char *) b, 1);
    } else if (number < 0x4000) {
      unsigned char b[2] = {(unsigned char) (((number >> 8) & 0x3F) | 0x80), (unsigned char) (number & 0xFF)};
      writeBytes((const char *) b, 2);
    } else if (number < 0x200000) {
      unsigned char b[3] = {(unsigned char) (((number >> 16) & 0x1F) | 0xC0), (unsigned char) ((number >> 8) & 0xFF),
                            (unsigned char) (number & 0xFF)};
      writeBytes((const char *) b, 3);
    } else if (number < 0x10000000) {
      unsigned char b[4] = {(unsigned char) (((number >> 24) & 0x0F) | 0xE0), (unsigned char) ((number >> 16) & 0xFF),
                            (unsigned char) ((number >> 8) & 0xFF), (unsigned char) (number & 0xFF)};
      writeBytes((const char *) b, 4);
    } else {
      unsigned char b[5] = {0xFF, (unsigned char) (number >> 24), (unsigned char) (number >> 16),
                            (unsigned char) (number >> 8), (unsigned char) (number)};
      writeBytes((const char *) b, 5);
    }
  }

  unsigned int PacketBuffer::readUVL() {
#ifdef PACKET_DEBUG
    return readU32();
#endif
    unsigned char b[5];
    readBytes((char *) b, 1);
    if ((b[0] & 0x80) == 0) {
      return b[0];
    } else if ((b[0] & 0x40) == 0) {
      readBytes((char *) b + 1, 1);
      return ((b[0] & 0x3F) << 8) + b[1];
    } else if ((b[0] & 0x20) == 0) {
      readBytes((char *) b + 1, 2);
      return ((b[0] & 0x1F) << 16) + (b[1] << 8) + b[2];
    } else if ((b[0] & 0x10) == 0) {
      readBytes((char *) b + 1, 3);
      return ((b[0] & 0x0F) << 24) + (b[1] << 16) + (b[2] << 8) + b[3];
    } else {
      readBytes((char *) b + 1, 4);
      return (b[1] << 24) | (b[2] << 16) | (b[3] << 8) | b[4];
    }
  }

  void PacketBuffer::writeSVL(int number) {
#ifdef PACKET_DEBUG
    return writeS32(number);
#endif
    if (number >= 0) {
      writeUVL((unsigned int) number << 1);
    } else {
      writeUVL((unsigned int) (-number - 1) << 1 | 1);
    }
  }

  int PacketBuffer::readSVL() {
#ifdef PACKET_DEBUG
    return readS32();
#endif
    unsigned int b = readUVL();
    if ((b & 1) == 0) {
      return b >> 1;
    } else {
      return -(b >> 1) - 1;
    }
  }

  void PacketBuffer::writeBytes(const char *bytes, int length) {
    reserveMore(length);
    memcpy(buffer + writePosition, bytes, length);
    writePosition += length;
  }

  void PacketBuffer::writeString(std::string str) {
    reserveMore(str.size());
    memcpy(buffer + writePosition, str.data(), str.size());
    writePosition += str.size();
  }

  void PacketBuffer::readBytes(char *bytes, int length) {
    if (readPosition + length > byteSize) throw BufferUnderflowException();
    memcpy(bytes, buffer + readPosition, length);
    readPosition += length;
  }

  std::string PacketBuffer::readString(int length) {
    std::string str(length, 0);
    if (readPosition + length > byteSize) throw BufferUnderflowException();
    memcpy((void *) str.data(), buffer + readPosition, length);
    readPosition += length;
    return str;
  }

  unsigned int PacketBuffer::size() {
    return byteSize;
  }

  void PacketBuffer::setSize(unsigned int sizep) {
    byteSize = sizep;
  }

  char* PacketBuffer::data() {
    return buffer;
  }

  bool PacketBuffer::end() {
    return readPosition>=byteSize;
  }

  bool PacketBuffer::more() {
    return readPosition<byteSize;
  }

  unsigned int PacketBuffer::remaining() {
    return byteSize - readPosition;
  }

  std::string PacketBuffer::getString() {
    return std::string(buffer, byteSize);
  }

  std::string PacketBuffer::getString(unsigned int p, unsigned int size) {
    return std::string(buffer + p, size);
  }

  void PacketBuffer::print() {
    for(unsigned int i = 0; i < byteSize; i++) {
      db_log("B %d = %d = %x",i,(unsigned char)buffer[i],(unsigned char)buffer[i]);
    }
  }

}

//
// Created by Michał Łaszczewski on 06/10/16.
//

#ifndef NET_PACKETBUFFER_H
#define NET_PACKETBUFFER_H

#include <string>
#include <cinttypes>
#include <exception>

namespace net {

  class PacketBuffer { /// TODO: convert to header only
  private:
    char *buffer;
    unsigned int bufferSize;
    unsigned int byteSize;
    unsigned int writePosition;
    unsigned int readPosition;
    bool external;

    void resizeBuffer(unsigned int bytes);

  public:
    PacketBuffer() :
      buffer((char *) malloc(2048)), bufferSize(2048), byteSize(0),
      writePosition(0), readPosition(0), external(false) {}
    PacketBuffer(unsigned int bufferSizep) :
      buffer((char *) malloc(bufferSizep)), bufferSize(bufferSizep),
      byteSize(bufferSizep), writePosition(0), readPosition(0), external(false) {}
    PacketBuffer(char* bufferp, unsigned int bufferSizep) :
      buffer(bufferp), bufferSize(bufferSizep),
      byteSize(bufferSizep), writePosition(0), readPosition(0), external(true) {}

    ~PacketBuffer() {
      if(!external) free(buffer);
    }

    void reset();

    void flip();

    void reserve(unsigned int bytes);

    void reserveMore(unsigned int bytes);

    unsigned int tellg();

    unsigned int tellp();

    void seekg(unsigned int p);

    void seekp(unsigned int p);

    void setU8(unsigned int p, unsigned char v);

    void setS8(unsigned int p, signed char v);

    void setU16(unsigned int p, unsigned short v);

    void setS16(unsigned int p, signed short v);

    void setU32(unsigned int p, unsigned int v);

    void setS32(unsigned int p, signed int v);

    void setU64(unsigned int p, std::uint64_t v);
    void setS64(unsigned int p, std::int64_t v);

    unsigned char getU8(unsigned int p);

    signed char getS8(unsigned int p);

    unsigned short getU16(unsigned int p);

    signed short getS16(unsigned int p);

    unsigned int getU32(unsigned int p);

    signed int getS32(unsigned int p);

    std::uint64_t getU64(unsigned int p);
    std::int64_t getS64(unsigned int p);

    std::string getString(unsigned int p, unsigned int size);

    char* getPointer(unsigned int p) {
      return buffer + p;
    }

    void writeU8(unsigned char v);

    void writeS8(signed char v);

    void writeU16(unsigned short v);

    void writeS16(signed short v);

    void writeU32(unsigned int v);

    void writeS32(signed int v);

    void writeU64(std::uint64_t v);
    void writeS64(std::int64_t v);

    unsigned char readU8();

    signed char readS8();

    unsigned short readU16();

    signed short readS16();

    unsigned int readU32();

    signed int readS32();

    std::uint64_t readU64();
    std::int64_t readS64();

    void writeUVL(unsigned int v);

    unsigned int readUVL();

    void writeSVL(int v);

    int readSVL();

    void writeBytes(const char *bytes, int length);

    void writeString(std::string str);

    void readBytes(char *bytes, int length);

    std::string readString(int length);

    char* readPointer(int length) {
      char* pointer = buffer + readPosition;
      readPosition += length;
      return pointer;
    }

    unsigned int size();
    char *data();
    void setSize(unsigned int size);

    bool end();
    bool more();
    unsigned int remaining();

    std::string getString();

    void print();
  };

  class BufferUnderflowException: public std::exception {
    virtual const char* what() const throw() {
      return "Buffer underflow";
    }
  };

}



#endif //NET_PACKETBUFFER_H

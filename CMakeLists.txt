cmake_minimum_required(VERSION 3.19)
project(observable_db_lmdb)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -Werror")

#THREADS
find_package (Threads)

#BOOST
find_package(Boost REQUIRED COMPONENTS program_options)
include_directories(${Boost_INCLUDE_DIRS})


add_executable(observable_db_lmdb main.cpp PacketBuffer.cpp Store.cpp Database.cpp taskQueue.cpp observation.cpp)
target_link_libraries(observable_db_lmdb lmdb)
target_link_libraries(observable_db_lmdb usockets ssl crypto z)
target_link_libraries(observable_db_lmdb ${Boost_LIBRARIES})
target_link_libraries(observable_db_lmdb ${CMAKE_THREAD_LIBS_INIT})

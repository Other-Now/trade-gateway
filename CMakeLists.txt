cmake_minimum_required(VERSION 3.20)
project(trade_gateway CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif()
add_compile_options(-Wall -Wextra -Wno-unused-parameter)

# Boost (header-only parts: Asio, Beast, JSON), libpq, OpenSSL.
# Locally these come from ~/tg-deps/root (scripts/deps.sh); in CI from apt.
find_path(BOOST_INCLUDE boost/beast.hpp REQUIRED)
find_path(PQ_INCLUDE libpq-fe.h PATH_SUFFIXES postgresql REQUIRED)
find_library(PQ_LIB pq REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Threads REQUIRED)

add_library(tgcore STATIC
  src/common/util.cpp
  src/common/json_impl.cpp
  src/gw/risk.cpp
  src/gw/pgwatch.cpp
  src/gw/hub.cpp
  src/gw/session.cpp
  src/gw/upstream.cpp)
target_include_directories(tgcore SYSTEM PUBLIC ${BOOST_INCLUDE} ${PQ_INCLUDE})
target_compile_definitions(tgcore PUBLIC BOOST_JSON_NO_LIB BOOST_ASIO_NO_DEPRECATED)
target_link_libraries(tgcore PUBLIC ${PQ_LIB} OpenSSL::SSL OpenSSL::Crypto Threads::Threads)

add_executable(tgw src/gw/main.cpp)
target_link_libraries(tgw tgcore)

add_executable(tg-mockex src/mockex/main.cpp)
target_link_libraries(tg-mockex tgcore)

add_executable(tg-bench src/bench/main.cpp)
target_link_libraries(tg-bench tgcore)

add_executable(tg-unit tests/unit.cpp)
target_link_libraries(tg-unit tgcore)

enable_testing()
add_test(NAME unit COMMAND tg-unit)

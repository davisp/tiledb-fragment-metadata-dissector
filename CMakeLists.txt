cmake_minimum_required(VERSION 3.27)

set(CMAKE_TOOLCHAIN_FILE
  "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
  CACHE STRING "Vcpkg toolchain file")
set(VCPKG_INSTALL_OPTIONS "--no-print-usage")

project("Fragment Metadata Dissector")

set(CMAKE_CXX_STANDARD 17)

set(SOURCES
  src/decompressor.cc
  src/fragment_metadata.cc
  src/main.cc
  src/reader.cc
  src/tile.cc)

find_package(aws-cpp-sdk-core REQUIRED)
find_package(ZLIB REQUIRED)

add_executable(fmd ${SOURCES})

target_include_directories(fmd PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(fmd PRIVATE aws-cpp-sdk-core)

target_link_libraries(fmd PRIVATE aws-cpp-sdk-core)
target_link_libraries(fmd PRIVATE ZLIB::ZLIB)

file(GLOB_RECURSE
  FORMAT_SOURCES
  LIST_DIRECTORIES false
  RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
  include/*.h
  src/*.cc)
list(SORT FORMAT_SOURCES)
add_custom_target(format
  WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
  COMMAND_EXPAND_LISTS
  COMMAND clang-format --verbose -i ${FORMAT_SOURCES})

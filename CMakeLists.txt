cmake_minimum_required(VERSION 3.27)

set(CMAKE_TOOLCHAIN_FILE
  "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
  CACHE STRING "Vcpkg toolchain file")

project("Fragment Metadata Dissector")

set(CMAKE_CXX_STANDARD 17)

set(SOURCES
  src/decompressor.cc
  src/main.cc
  src/reader.cc
  src/tile.cc)

add_executable("fmd" ${SOURCES})
target_include_directories("fmd" PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_link_libraries("fmd" "z")

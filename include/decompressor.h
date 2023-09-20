#pragma once

#include <cstdint>
#include <vector>

#include "tile.h"

void tdb_decompress(DiskLayout& layout, uint8_t* buf, size_t nytes);

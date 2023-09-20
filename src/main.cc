
#include <stdio.h>
#include <unistd.h>
#include <iostream>

#include "fragment_metadata.h"
#include "reader.h"
#include "s3.h"

// Hard coded values from the array schema
#define NUM_FIELDS 15

int main(int argc, char* argv[]) {
  S3Initializer s3ctx;

  if (argc != 2) {
    fprintf(stderr, "usage: %s FRAGMENT_METADATA_FILE\n", argv[0]);
    exit(1);
  }

  try {
    S3 s3;

    std::string uri =
        "s3://tiledb-norman/overture_buildings/__fragments/"
        "__1694461864215_1694464554777_b9fb28aaa63c4f6182661c9e00e39481_19/"
        "__fragment_metadata.tdb";

    auto data = s3.read(uri, 0, 18194);

    Reader reader(data);
    FragmentMetadata fmd(reader, NUM_FIELDS);

  } catch (std::exception& exc) {
    std::cerr << "Uncaught error: " << exc.what() << std::endl;
  }
}

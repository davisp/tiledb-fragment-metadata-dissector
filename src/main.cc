
#include <stdio.h>
#include <unistd.h>

#include "fragment_metadata.h"
#include "reader.h"

// Hard coded values from the array schema
#define NUM_FIELDS 15

int
main(int argc, char* argv[])
{
  if (argc != 2) {
    fprintf(stderr, "usage: %s FRAGMENT_METADATA_FILE\n", argv[0]);
    exit(1);
  }

  Reader reader(argv[1]);
  FragmentMetadata fmd(reader, NUM_FIELDS);
  fmd.dump();

  reader.show_read_report();
}

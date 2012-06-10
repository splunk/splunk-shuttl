#!/bin/bash

# Tests whether a script runs ant without having ant in the PATH variable.

script_dir=$(dirname $0)
it_file=$1

PATH_ORIGINAL=$PATH
# Set path to be minimal and it should not contain ant
export PATH=/bin
it_file_output=$($script_dir/../../../$it_file) &> /dev/null

# Restore path
export PATH=$PATH_ORIGINAL
# If it finds build path, then it ran ant.
finds_build_file=`echo "$it_file_output" | grep -P "Buildfile: .*?build.xml"`

echo # Extra echo for nicer test output.

if [ "$finds_build_file" = "" ]; then
  echo "FAIL: $it_file did not run build file"
else
  echo "SUCCESS: $it_file ran build file."
fi

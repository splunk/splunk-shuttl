#!/bin/bash

script_dir=$(dirname $0)
it_file=$1

# Setup without ant
PATH_ORIGINAL=$PATH
export PATH=/bin
#Test
it_file_output=$($script_dir/../../../$it_file) &> /dev/null

export PATH=$PATH_ORIGINAL
finds_build_file=`echo "$it_file_output" | grep -P "Buildfile: .*?build.xml"`

# Extra echo for nicer test output.
  echo 

if [ "$finds_build_file" = "" ]; then
  echo "FAIL: $it_file did not run build file"
else
  echo "SUCCESS: $it_file ran build file."
fi

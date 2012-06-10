#!/bin/bash

script_dir=$(dirname $0)

# Setup
export PATH=/usr/bin:/usr/local/bin:/bin
#Test
$script_dir/../../testit.sh

exit_status=$?

# Extra echo for nicer test output.
  echo 
if [ $exit_status != 0 ]; then
  echo "FAIL: testit.sh did not exit with zero."
else
  echo "SUCCESS: buildit.sh exited with zero."
fi

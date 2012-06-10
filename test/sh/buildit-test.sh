#!/bin/bash

script_dir=$(dirname $0)

# Setup
export PATH=/usr/bin:/usr/local/bin:/bin
#Test
$script_dir/../../buildit.sh

#Exit with the same exitstatus as buildit
exit_status=$?

# Extra echo for nicer test output.
  echo 
if [ $exit_status != 0 ]; then
  echo "FAIL: buildit.sh did not exit with zero."
else
  echo "SUCCESS: buildit.sh exited with zero."
fi

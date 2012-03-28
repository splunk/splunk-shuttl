# THESE TESTS ARE DEPRECATED. PLEASE DO NOT ADD ADDITIONAL SHELL TESTS
# ADD TESTNG OR JUNIT TESTS INSTEAD

#!/bin/bash
# The first argument ($1) should contain HADOOP_HOME
# The second argument ($2) should contain SHEPDIR
# The third argument ($3) should contain SPLUNK_HOME
# The forth argument ($4) should contain SPLUNK_USERNAME
# The fifth argument ($5) should contain SPLUNK_PASSWORD

print_usage () {
  echo "Usage: $filename <HADOOP_HOME> <SHEPDIR> <SPLUNK_HOME> <SPLUNK_USERNAME> <SPLUNK_PASSWORD>."
}

filename=$(basename $0)
hadoop_home_arg=$1
shepdir_arg=$2
splunk_home_arg=$3
splunk_username_arg=$4
splunk_password_arg=$5

if [ $# -ne 5 ]; then
  echo "Warning: $filename was not run correctly."
  print_usage
  exit
fi

results_out=""
fails_out=""

main() {
  setup_environment
  setup_tests
  run_tests
}

setup_environment() {
  set_env_with_variable HADOOP_HOME $hadoop_home_arg $HADOOP_HOME
  set_env_with_variable SHEPDIR $shepdir_arg $SHEPDIR
  set_env_with_variable SPLUNK_HOME $splunk_home_arg $SPLUNK_HOME
  set_env_with_variable SPLUNK_USERNAME $splunk_username_arg $SPLUNK_USERNAME
  set_env_with_variable SPLUNK_PASSWORD $splunk_password_arg $SPLUNK_PASSWORD
}

# Sets the environment variable in $1 with value in $2.
# $3 contains the current value of $1.
# Warning will be printed if there's already a value in variable $3.
set_env_with_variable () {
  env_name=$1
  new_value=$2
  old_value=$3
  if [ "$old_value" != "" ] && [ "$old_value" != "$new_value" ]; then
    echo "Warning: Overriding environment variable $env_name=$old_value with $new_value."
  fi
  export $env_name=$new_value
}

setup_tests() {
  script_dir=$(dirname $0)
  out_dir=$script_dir/test-output
  mkdir -p $out_dir

  results_out=$out_dir/test-results.txt
  fails_out=$out_dir/test-fails.txt

  clear_file $results_out
  clear_file $fails_out
}

clear_file() {
  echo -n "" > $1
}

run_tests() {
  nr_tests=0
  for test in `find ./test -name runtest.sh`
  do
    run_test $test
    nr_tests=$((nr_tests+1))
  done
  for test in `find . -name runtest_phase2.sh`
  do
    run_test $test
    nr_tests=$((nr_tests+1))
  done

  failed_tests=`wc -l $fails_out | grep -oP "\d+? "`

  echo "Total shell tests run: $nr_tests, Failures: $failed_tests"
  echo "Test output, fails and successes can be found in $out_dir"
}

run_test() {
  echo "-- Started running test: $test --" >> $results_out
  $test >> $results_out 2>&1
  if [[ $? != 0 ]]; then
    echo "Fail in test: $test" | tee -a $fails_out | tee -a $results_out
  else
    echo "Win in test: $test" | tee -a $results_out
  fi
  echo "-- Finished running test: $test --" >> $results_out
  echo "-- ---------------------" >> $results_out
}

main #run the script

#!/bin/bash

results_out=""
fails_out=""
wins_out=""

main() {
  setup
  run_tests
}

setup() {
  script_dir=$(dirname $0)
  out_dir=$script_dir/test-output
  mkdir -p $out_dir

  results_out=$out_dir/test-results.txt
  fails_out=$out_dir/test-fails.txt
  wins_out=$out_dir/test-wins.txt

  clear_file $results_out
  clear_file $fails_out
 clear_file $wins_out
}

clear_file() {
  echo -n "" > $1
}

run_tests() {
  nr_tests=0
  for test in `find . -name runtest.sh`
  do
    $test &> $results_out
    if [[ $? != 0 ]]; then
    echo "Fail in test: $test" | tee -a $fails_out
    else
    echo "Win in test: $test" | tee -a $wins_out
    fi
    nr_tests=$((nr_tests+1))
  done

  failed_tests=`wc -l $fails_out | grep -oP "\d+? "`

  echo "Total shell tests run: $nr_tests, Failures: $failed_tests"
  echo "Test output, fails and successes can be found in $out_dir"
}

main #run the script

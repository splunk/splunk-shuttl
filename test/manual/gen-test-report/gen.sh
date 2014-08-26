#!/bin/bash -eu

(echo "<xml>"
 for testresults in `ls build/test-results/*/testng-results.xml`; do
     echo "<path dir=\"`dirname $testresults`\">"
     cat $testresults | grep "</*class\|</*test-method"
     echo "</path>"
 done
 echo "</xml>")


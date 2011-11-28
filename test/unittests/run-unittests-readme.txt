Here's how to use run-unittests.sh

run-unittests.sh is asserts that the unit tests meet the following conditions:
1. Test live in a sub directory of test/unittest
2. Test is named runtest.sh
3. Use exit status 0 for executed successfully, any other exit status for failiure
4. Unit tests take no arguments

You can use both stdout and stderr to log output from the tests.

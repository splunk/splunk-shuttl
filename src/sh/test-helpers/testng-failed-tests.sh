#!/bin/bash
find . -name "testng-results.xml" -exec grep -H "FAIL" \{\} \;

#!/bin/bash

dd if=/dev/urandom bs=$(( 1024 * 1024 )) count=1 | base64 | sed 's/[AOEIU]/,/g' | tr , '\n'

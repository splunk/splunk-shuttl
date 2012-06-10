#!/bin/bash

set -e
set -u

shuttl_dir=$(dirname $0)

source $shuttl_dir/src/sh/set-ant-env.sh
ant clean-all test-all

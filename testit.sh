#!/bin/bash

set -e
set -u

shuttl_dir=$(/usr/bin/dirname $0)

source $shuttl_dir/src/sh/set-ant-env.sh $shuttl_dir
ant clean-all test-all

#!/bin/bash

script_dir=$(dirname $0)

cp $script_dir/../git-hooks/* $script_dir/../../.git/hooks

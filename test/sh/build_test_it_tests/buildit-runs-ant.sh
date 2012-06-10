#!/bin/bash

# Tests if buildit.sh runs ant

script_dir=$(dirname $0)

$script_dir/it-file-runs-ant.sh buildit.sh

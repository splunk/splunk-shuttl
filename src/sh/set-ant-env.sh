#!/bin/bash

export SHUTTLDIR=$(cd $(dirname $0)/../.. && pwd)

## Setup ant
export ANT_HOME="$SHUTTLDIR/contrib/apache-ant-1.8.2"

# Setting our ant before everyone elses.
export PATH="$ANT_HOME/bin:$PATH"

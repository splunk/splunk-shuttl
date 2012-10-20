#!/bin/bash

SCRIPT_DIR=$(dirname $0)
SHUTTL_HOME=`$SCRIPT_DIR/print-shuttl-home.sh`

#Takes the args and xargs it to ant. Example: call_ant_with_string_args "-Dx=1 -Dy=2"
call_ant_with_string_args() {
  echo "$1" | xargs ant -f $SHUTTL_HOME/build.xml 
}


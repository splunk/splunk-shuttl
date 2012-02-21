#!/bin/sh
#set -x
#
# hiv3-kill.sh - 	kill all processes with "hive" in it
# 			this script is named as such so it doesn't kill itself 
#
mylist=`ps -ef | grep hive | awk '{print $2}'`
echo "List of pids to kill: ${mylist}"
for i in $mylist
do
    kill $i
done

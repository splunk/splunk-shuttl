#!/bin/bash

/sbin/ifconfig | grep "inet.*broadcast" | awk '{print $2}'

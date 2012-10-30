#!/bin/bash

ifconfig | grep "inet.*broadcast" | awk '{print $2}'

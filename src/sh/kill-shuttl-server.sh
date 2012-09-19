#!/bin/bash

ps -ef | 
  grep -P "[a-z]+\.ShuttlJettyServer" | # Grep the ShuttlJettyServer process
  awk '{ print $2}' | # Print the pid
  xargs kill # Kill the pid

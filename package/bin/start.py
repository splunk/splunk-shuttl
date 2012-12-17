#!/usr/bin/python

import subprocess
import signal
import os

splunk_home = os.environ['SPLUNK_HOME']
os.chdir(splunk_home + "/etc/apps/shuttl/bin")

start_shuttl_server = "exec $JAVA_HOME/bin/java -Djetty.home=. -Dsplunk.home=../../../../ -cp .:../lib/*:./* com.splunk.shuttl.server.ShuttlJettyServer"

process = subprocess.Popen(start_shuttl_server, shell=True, stdout=subprocess.PIPE)
print("Started Shuttl pid: " + str(process.pid))

def handle_signals(a, b):
    print("Will kill Shuttl [" + str(process.pid) + "]")
    process.kill()

signal.signal(signal.SIGTERM, handle_signals)
signal.signal(signal.SIGQUIT, handle_signals)
signal.signal(signal.SIGINT, handle_signals)

print("Waiting for Shuttl [" + str(process.pid) + "]")
process.wait()

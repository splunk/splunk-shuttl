import subprocess
import splunk.Intersplunk
import sys

process = subprocess.Popen('java -cp $HADOOP_HOME/hadoop-core-0.20.203.0.jar:$HADOOP_HOME/conf:$HADOOP_HOME/lib/commons-logging-1.1.1.jar:$HADOOP_HOME/lib/commons-configuration-1.6.jar:$HADOOP_HOME/lib/commons-lang-2.4.jar:$HADOOP_HOME/lib/splunk-hadoop-connector-0.4.1.jar:$SPLUNK_HOME/etc/apps/search/bin  com.splunk.shep.customsearch.HDFSOut ' + sys.argv[1], shell=True, stdin=subprocess.PIPE)
# output results
results,unused1,unused2 = splunk.Intersplunk.getOrganizedResults()

for r in results:
	process.stdin.write(r["_raw"])
	process.stdin.write("\n")
process.stdin.write("exit")
process.stdin.write("\n")

splunk.Intersplunk.outputResults(results)

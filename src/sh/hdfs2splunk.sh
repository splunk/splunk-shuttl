#/bin/sh
#set -x

# This will transfer a single file from Hadoop HDFS to Splunk index
# This has several config params. Before you use this, be sure to update them.


# UPDATE THIS SECTION (begin)

# hostname of the name node
HADOOP_NN_HOST=localhost
# port of the name node
HADOOP_NN_PORT=9000
# your splunk home dir
SPLUNK_HOME=./splunk
# your hadoop home dir
HADOOP_HOME=./hadoop-1.0.0
# tmp dir where the files from hdfs will be copied first before oneshotting
TMPDIR=/tmp/hdfs2splunk

# this is the index to put the data in, make sure it exists in Splunk
SPL_INDEX="testindex"

# UPDATE THIS SECTION (end)

# the src file you want to copy
HDFSFILENAME=$1
SCRIPTNAME=`basename $0`

# the tmp local file that will be "one shotted" into splunk
LOCALFILENAME=${TMPDIR}/`basename ${HDFSFILENAME}`
# full url to the file in hdfs
HDFSURL=hdfs://${HADOOP_NN_HOST}:${HADOOP_NN_PORT}/${HDFSFILENAME}

# the following are splunk metadata values for the data being indexed

# the sourcetype name
SPL_SOURCETYPE="hdfsfile"
# host value is being set to the name node host
SPL_HOST=${HADOOP_NN_HOST}
# source is the hdfs url to the file 
SPL_SOURCE=${HDFSURL}

if [ -z $1 ]; then
	echo "usage: ${SCRIPTNAME} <filename in hdfs>"
	exit 1
fi
if [ ! -d ${TMPDIR} ]; then
	echo "${SCRIPTNAME}: making ${TMPEDIR}"
	mkdir -p ${TMPDIR}
fi

echo "${SCRIPTNAME}: Copying file from ${HDFSURL}..."
${HADOOP_HOME}/bin/hadoop distcp -update  ${HDFSURL} file://${TMPDIR}
echo "${SCRIPTNAME}: Copied data to ${LOCALFILENAME}"
if [ ! -f ${LOCALFILENAME} ]; then
	echo "${SCRIPTNAME}: ${LOCALFILENAME} not found"
	exit 1
fi	
echo "${SCRIPTNAME}: One Shotting to Splunk..."

# 
# ${SPLUNK_HOME}/bin/splunk add oneshot ${LOCALFILENAME} -sourcetype "${SPL_SOURCETYPE}" -host "${SPL_HOST}" -rename-source "${HDFSURL}" -index "${SPL_INDEX}"

# undocumented "nom on" command
${SPLUNK_HOME}/bin/splunk nom on ${LOCALFILENAME} -sourcetype "${SPL_SOURCETYPE}" -host "${SPL_HOST}" -rename-source "${HDFSURL}" -index "${SPL_INDEX}"

# uncomment out the following if you want to delete after indexing
# echo "${SCRIPTNAME}: deleting ${LOCALFILENAME}"
# rm -rf ${LOCALFILENAME} 
echo "${SCRIPTNAME}: done"

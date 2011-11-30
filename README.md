
Shep - Integration of Splunk and Hadoop
=======================================

Shep is a collection of integration modules to enable
seamless data flow and processing between Splunk, the
NoSQL platform for machine data, and Hadoop.

If you are not in the beta program, please register here:

* http://www.splunk.com/goto/hadoop-beta

For documentation, see:

* http://docs.splunk.com/Documentation/Hadoop

Key features as of v0.4 are:

* Streaming of data via Splunk Forwarding to HDFS
* SplunkInputFormat and SplunkOutputFormat classes for use in Hadoop
* Automatic batch rolling of indexed data to HDFS

Note: earlier versions of the forwarding mechanism relied on
Flume. That dependency is now removed, and Shep will write
direct to HDFS without a Flume intermediary.

Prerequisites
-------------

### Hadoop

Currently the Hadoop version used is 0.20.203.0

You can download it from one of the mirror sites listed [here][hadoop-download].
And see the [Hadoop documentation][] for instructions on installing and more.

[hadoop-download]:http://www.apache.org/dyn/closer.cgi?path=hadoop/core/hadoop-0.20.203.0/hadoop-0.20.203.0rc1.tar.gz
[Hadoop documentation]:http://hadoop.apache.org/common/docs/r0.20.203.0/

### Splunk

Currently the Splunk version used is 4.2.4

You can download it from [here][splunk-download]
And see the [Splunk documentation][] for instructions on installing and more.

[Splunk documentation]:http://docs.splunk.com/Documentation/Splunk/latest/User
[splunk-download]:http://www.splunk.com/download


Building from Source
--------------------

### Requirements

All you need is:

* Java JDK 6

The build should work on both MacOSX and Linux.

Environment setup:

	% source setjavaenv

Building Shep:

	% ./buildit.sh

or

	% ant

If everything goes well, you should see the message

	BUILD SUCCESSFUL

### Eclipse Users

You'll need to build once, before you can use Eclipse

### Coding Conventions

The Shep code base is to follow the standard java conventions:
http://java.sun.com/docs/codeconv/html/CodeConvTOC.doc.html


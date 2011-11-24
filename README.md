
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

Building from Source
----------

### Requirements

All you need is:

* Java JDK 6
* git

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



Shep - Integration of Splunk and Hadoop
=======================================

Shep is a collection of integration modules to enable
seamless data flow and processing between Splunk, the
NoSQL platform for machine data, and Hadoop.

If you are not in the beta program, please register here:

* http://www.splunk.com/goto/hadoop-beta

For documentation, see:

* http://docs.splunk.com/Documentation/Hadoop

Key features as of v0.4.1 are:

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

Getting Started
---------------

Set the `JAVA_HOME` environment variable

Make sure that you can run `ssh localhost` without having to enter a password*

Cd into to the splunk-shep directory and run the following:

Put a .tgz packaged Splunk in the put-splunk-tgz-here directory**

Build shep:

	$ ./buildit.sh

Run the tests:

	$ ant test

* Here's how you setup passphraseless ssh: http://hadoop.apache.org/common/docs/current/single_node_setup.html#Setup+passphraseless
** You don't need to do this if you want to run against your own Splunk instance.

### Running tests against your own Splunk and/or Hadoop

Warning: All of your Splunk indexes is cleared if you do this.

Assertions: The tests assert that the username and password for your Splunk instance is admin and password. We also assert that your Hadoop namenode has been formatted.

How to do it:

Set `SPLUNK_HOME` and/or `HADOOP_HOME` environment variables

Run ant test with parameter `-DgotSplunk=true` and/or `-DgotHadoop=true`. For example:

	$ ant test -DgotSplunk=true -DgotHadoop=true

The script will now use your own environment variables to run the tests. You don't have to run with both parameters. You can run with either one.

### Specifying which Hadoop version to run

Run ant test with parameter `-DhadoopVersion=<version>`, like so:

	$ ant test -DhadoopVersion=203

Currently supports version 203 and 205 for downloading. Tests are not working on 205 yet.

### Specifying Splunk username and password

If you're running the tests against external Splunk instances with -DgotSplunk=true, you can specify username and password with ant arguments -DsplunkUsername=<username> -DsplunkPassword=<password>, like so:

	$ ant test -DgotSplunk=true -DsplunkUsername=admin -DsplunkPassword=secret

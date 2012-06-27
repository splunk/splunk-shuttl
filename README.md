
Shuttl - Archiving for Splunk 
=======================================

Splunk is the premier technology for gaining Operational Intelligence on Machine Data. Since it
can handle large volume of data at a fast rate, often times users will only want to analyze
recent data, and data that is beyond a certain range is archived.

Splunk provides hooks for allowing the administrator to designate archiving policies and
actions. However, the actions are entirely implemented by the administrator of the system.

Shuttl provides a full-lifecycle solution for data in Splunk.

It can:
* manage the transfer of data from Splunk to an archive system
* enable an administrator to inventory/search the archive
* allow an administrator to selectively restore archived data into "thawed"
* remove archived data from thawed

This works on the following systems
* Attached storage
* HDFS
* S3 (in theory)

Prerequisites
-------------

### Hadoop

Currently the Hadoop version used is 1.0.3

You can download it from one of the [mirror sites][hadoop-download].
And see the [Hadoop documentation][] for instructions on installing and more.

[hadoop-download]:http://www.apache.org/dyn/closer.cgi?path=hadoop/core/hadoop-1.0.3
[Hadoop documentation]:http://hadoop.apache.org/common/docs/r1.0.3

### Splunk

Currently the Splunk version used is 4.3.3

You can download it [Splunk][splunk-download].  And see the [Splunk documentation][] for instructions on installing and more.

[Splunk documentation]:http://docs.splunk.com/Documentation/Splunk/latest/User
[splunk-download]:http://www.splunk.com/download

### Java

* Java JDK 6

Development
--------------

### Eclipse Users

You'll need to build once, before you can use Eclipse
This .eclipse.templates directory contains templates for generating Eclipse files to configure
Eclipse for shuttl development.


### Coding Conventions

The Shuttl code base is to follow the standard java conventions except for using braces for all for-loops, if-statements etc. We try to not use braces to avoid too much indentation. We rely on having tests to catch any mistake done by forgetting braces, when having more than one line after an if-statement or for-loop.

The standard java conventions can be found here:
http://java.sun.com/docs/codeconv/html/CodeConvTOC.doc.html

Getting Started
---------------

Ensure that:
* `JAVA_HOME` environment variable is defined correctly
* Make sure that you can run `ssh localhost` without having to enter a password
* Make sure you have a tgz package of Splunk in the directory put-splunk-tgz-here (not needed if you are using your own Splunk instance, see below)

Build shuttl:

	$ ./buildit.sh

Run the tests:

	$ ant test-all

### How to Setup Passphraseless SSH

Here's how you setup passphraseless ssh: http://hadoop.apache.org/common/docs/current/single_node_setup.html#Setup+passphraseless

### Test configuration

Create a file called `build.properties`

Copy the contents from `default.properties` to `build.properties` and edit the values you want to change

### Running tests against your own Splunk and/or Hadoop

Warning: All of your Splunk indexes is cleared if you do this

Assertions: The tests assert that your Hadoop namenode has been formatted

How to do it:

Set `SPLUNK_HOME` and/or `HADOOP_HOME` environment variables

In your `build.properties`, set the properties `defined.means.running.on.self.defined.splunk.home` and/or `defined.means.running.on.self.defined.hadoop.home` to any value

Now run:

	$ `ant test-all`

The script will now use your own environment variables to run the tests. You don't have to run with both properties defined. You can run with either one

### Specifying which Hadoop version to run

In your `build.properties`, set the property `hadoop.version` to the version you want to run

Now run:

	$ `ant clean-all`
	$ `ant test-all`

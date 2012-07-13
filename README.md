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

License
---------

Shuttl is licensed under the Apache License 2.0. Details can be found in the LICENSE file.

Shuttl is an unsupported community open source project and therefore is subject to being incomplete and containing bugs. 

The Apache License only applies to Shuttl and no other Splunk software is implied.

Splunk, in using the Apache License, does not provide any warranties or indemnification, and does not accept any liabilities with the use of Shuttl.

We are now accepting contributions from individuals and companies to our Splunk open source projects.


Prerequisites
-------------

### Splunk

Currently the Splunk version used is 4.3.3

You can download it [Splunk][splunk-download].  And see the [Splunk documentation][] for instructions on installing and more.

[Splunk documentation]:http://docs.splunk.com/Documentation/Splunk/latest/User
[splunk-download]:http://www.splunk.com/download

### Java

* Java JDK 6

### Hadoop (optional)

This is needed if you are using HDFS/S3. Currently the Hadoop version used is 1.0.3

You can download it from one of the [mirror sites][hadoop-download].
And see the [Hadoop documentation][] for instructions on installing and more.

[hadoop-download]:http://www.apache.org/dyn/closer.cgi?path=hadoop/core/hadoop-1.0.3
[Hadoop documentation]:http://hadoop.apache.org/common/docs/r1.0.3


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


Installing the app
------------------

Here's how to install the Shuttl app in your Splunk instance. Shuttl comes with some pre-configured values that you might need to modify.

### Install
1. Build the app by running `ant dist`
2. Extract the build/shuttl.tgz in your $SPLUNK_HOME/etc/apps/
3. While Splunk is not running, configure Shuttl and Splunk as mentioned below
4. Start Splunk up, and enable the Shuttl App via the Manager
5. If the index is getting data, and calling the archiver, then you should see the data in HDFS

### Shuttl Configuration
There are two configuration files that you might care about. One for archiving and one for the Shuttl server. They both live in the shuttl/conf directory. All the values are populated with default values to serve as an example.

The archiver.xml:
- localArchiverDir: A local path (or an uri with file:/ schema) where shuttl's archiver's temporary transfer data, locks, metadata, etc. is stored.
- archiverRootURI: An URI where to archive the data. Currently supports the "hdfs://" and "file:/" schemas.
- clusterName: Unique name for your Splunk cluster. Use the default if you don't care to name your cluster for each Shuttl installation. Note, this is only a Shuttl concept for a group of Splunk indexers that should be treated as a cluster. Splunk does not have this notion.
- serverName: This is the Splunk Server Name. Check Splunk Manager for that server to populate this value. Must be unique per Shuttl installation.
- archiveFormats: The formats to archive the data as. The current available formats are SPLUNK_BUCKET and CSV. You can configure Shuttl to archive your data as both formats.

The server.xml:
- httpHost: The host name of the machine. (usually localhost)
- httpPort: The port for the shuttl server. (usually 9090)

Note, the directory that the data will be archived to is
	[archiverRootURI]/archive_data/[clusterName]/[serverName]/[indexName]

### Splunk Index Configuration

In addition, you need to configure Splunk to call the archiver script (set coldToFrozenScript) for each index that is being archived. You can do this by creating an indexes.conf file in $SPLUNK_HOME/etc/apps/shuttl/local with the appropriate config stanzas. An example is as follows:


	[mytest]
	homePath = $SPLUNK_DB/mytest/db
	coldPath = $SPLUNK_DB/mytest/colddb
	thawedPath = $SPLUNK_DB/mytest/thaweddb
	rotatePeriodInSecs = 10
	frozenTimePeriodInSecs = 120
	maxWarmDBCount = 1
	coldToFrozenScript = "$SPLUNK_HOME/etc/apps/shuttl/bin/archiveBucket.sh mytest"

Note: Note the repeat of "mytest" as an argument to the coldToFrozenScript. This should always match the index name.

WARNING: the settings rotatePeriodInSecs, frozenTimePeriodInSecs, maxWarmDBCount are there only for testing to verify that data can be successfully transfered by inducing rapid bucket rolling. Don't use in production. See [Set a retirement and archiving policy](http://docs.splunk.com/Documentation/Splunk/latest/admin/Setaretirementandarchivingpolicy) and [Indexes.conf](http://docs.splunk.com/Documentation/Splunk/4.3.3/admin/Indexesconf) documentation to suit your test and deployment needs. Expected usage in production is that maxDataSize correspond to a HDFS block or larger (splunk default is 750mb), and maxHotIdleSecs should be set to 86400 for buckets approximately 24hrs worth of data.

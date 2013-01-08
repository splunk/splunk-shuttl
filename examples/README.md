Shuttl - Archiving for Splunk - In 15 min
=====================================================

This is a quickstart guide for shuttl and a few different backends. Below is a guide on how to install and configure shuttl. You will also find examples of the configuration files in the neighbouring folders.

Prerequisites
-------------

### Splunk

The currently used Splunk version is 5.0.x. 
Shuttl has support for Splunk Clustering.

You can download it [Splunk][splunk-download]. And see the [Splunk documentation][] for instructions on installing and more.

[Splunk documentation]:http://docs.splunk.com/Documentation/Splunk/latest/User
[splunk-download]:http://www.splunk.com/download

### Java

* Java JDK 6

### Hadoop (optional)

This is needed if you are using HDFS. Currently Hadoop 1.1.1 is used.

You can download it from one of the [mirror sites][hadoop-download]. 
And see the [Hadoop documentation][] for instructions on installing and more.

[hadoop-download]:http://www.apache.org/dyn/closer.cgi?path=hadoop/core/hadoop-1.1.1
[Hadoop documentation]:http://hadoop.apache.org/common/docs/r1.1.1

Getting Started
---------------

Ensure that:
* `JAVA_HOME` environment variable is defined correctly.
* you can run `ssh localhost` without having to enter a password.

### How to Setup Passphraseless SSH

Here's how you setup passphraseless ssh: http://hadoop.apache.org/common/docs/current/single_node_setup.html#Setup+passphraseless  

Installing the app
------------------

Here's how to install the Shuttl app in your Splunk instance. Shuttl comes with some pre-configured values that you might need to modify.

### Installing from splunkweb
1. Start splunkweb.
2. Navigate to App -> Find more apps...
3. Seach for shuttl.
4. Select 'Install free' and follow the instructions.

### Installing from git
1. Run git clone https://github.com/splunk/splunk-shuttl.git.
2. Run ./buildit in the splunk-shuttl folder.
3. Extract the build/shuttl.tgz in your $SPLUNK_HOME/etc/apps/ folder.
4. While Splunk is not running, configure Shuttl and Splunk as mentioned below.
5. Start Splunk up, and enable the Shuttl via the App section under Manager.

Shuttl Configuration
--------------------

Firstly you want to configure your splunk index for shuttl. This is best done by creating a shuttl/local folder and creating the file indexes.conf within it or copy the one from default/ and modify it to your specifications.

Secondly there are another three configuration files that you might care about. One for archiving, one for Splunk and one for the Shuttl server. They all live in the shuttl/conf directory. All the values are populated with default values to serve as an example.

In addition to these configuration files, there are property files for the backends. These live in shuttl/conf/backend directory. These need to be configured as well depending on the backendName you choose.

### Splunk Index Configuration (local/indexes.conf)

You need to configure Splunk to call the archiver script (setting the coldToFrozenScript and/or warmToColdScript) for each index that is being archived. You can do this by creating an indexes.conf file in $SPLUNK_HOME/etc/apps/shuttl/local with the appropriate config stanzas. An example is as follows:

	[mytestindex]
	homePath = $SPLUNK_DB/mytestindex/db 
	coldPath = $SPLUNK_DB/mytestindex/colddb 
	thawedPath = $SPLUNK_DB/mytestindex/thaweddb 
	rotatePeriodInSecs = 10 
	maxWarmDBCount = 1 
	maxDataSize = 10000
	maxTotalDataSizeMB = 10000
	warmToColdScript = $SPLUNK_HOME/etc/apps/shuttl/bin/warmToColdScript.sh 
	coldToFrozenScript = $SPLUNK_HOME/etc/apps/shuttl/bin/coldToFrozenScript.sh 
	
For the full index configuration options see [indexconf][].

[indexconf]:http://docs.splunk.com/Documentation/Splunk/latest/admin/Indexesconf

### conf/archiver.xml

- localArchiverDir: A local path (or an uri with file:/ schema) where shuttl's archiver's temporary transfer data, locks, metadata, etc. is stored.
- backendName: The of the backend you want to use. Currently supports: local, hdfs, s3, s3n and glacier.
- archivePath: The absolute path in the archive where your files will be stored. Required for all backends.
- clusterName: Unique name for your Splunk cluster. Use the default if you don't care to name your cluster for each Shuttl installation. Note, this is only a Shuttl concept for a group of Splunk indexers that should be treated as a cluster. Splunk does not have this notion.
- serverName: This is the Splunk Server Name. Check Splunk Manager for that server to populate this value. Must be unique per Shuttl installation.
- archiveFormats: The formats to archive the data as. The current available formats are SPLUNK_BUCKET, CSV and SPLUNK_BUCKET_TGZ. You can configure Shuttl to archive your data as all formats at the same time, which you can use for different use cases.
* Warning: The old archiverRootURI is deprecated. It will still work for right now, but we recommend that you use the new configuration with property files instead.

Example for local storage:

	<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
	<ns2:archiverConf xmlns:ns2="com.splunk.shuttl.server.model">
	   <localArchiverDir>file:/~/testArea/shuttl_archiver</localArchiverDir>
	    <!-- Supported values for backend: local, hdfs, s3, s3n or glacier -->
	    <backendName>local</backendName>
	    <!-- Path on the backend where Shuttl will store data -->
	    <archivePath>/user/testUser/testArea</archivePath>
	
	    <clusterName>clusterName</clusterName>
	    <serverName>localhost</serverName>
	  <!-- Three example formats -->
	  <archiveFormats>
	    <archiveFormat>SPLUNK_BUCKET</archiveFormat>
	    <archiveFormat>SPLUNK_BUCKET_TGZ</archiveFormat>
	    <archiveFormat>CSV</archiveFormat>
	  </archiveFormats>
	</ns2:archiverConf>

### conf/server.xml

- httpHost: The host name of the machine. (usually localhost)
- httpPort: The port for the shuttl server. (usually 9090)

Example for local storage:

	<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
	<ns2:serverConf xmlns:ns2="com.splunk.shuttl.server.model">
	  <httpHost>localhost</httpHost>
	  <httpPort>9090</httpPort>
	</ns2:serverConf>

### conf/splunk.xml

- host: The host name for the splunk instance where Shuttl is installed. Should be localhost
- port: The management port for the splunk server. (Splunk defaults to 8089)
- username: Splunk username
- password: Splunk password

Example for local storage:

	<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
	<ns2:splunkConf xmlns:ns2="com.splunk.shuttl.server.model">
	  <host>localhost</host>
	  <port>8089</port>
	  <username>admin</username>
	  <password>changeme</password>
	</ns2:splunkConf>

### conf/backend/hdfs.properties (required for hdfs)
- hadoop.host: The host name to the hdfs name node.
- hadoop.port: The port to the hdfs name node.

    	hadoop.host = NAMENODE_IP	
    	hadoop.port = HDFS_NAMENODE_PORT

### conf/backend/amazon.properties (required for s3, s3n or glacier)
- aws.id: Your Amazon Web Services ID
- aws.secret: Your Amazon Web Services secret
- s3.bucket: Bucket name for storage in s3
- glacier.vault: The vault name for storage in glacier.
- glacier.endpoint: The server endpoint to where the data will be stored. (i.e. https://glacier.us-east-1.amazonaws.com/)
* Note: The glacier backend currently uses both glacier and s3, so s3.bucket is still required when using glacier. This is also the reason why archivePath is always required.
	
	
    	\# AWS access keys, which you get it from the aws console.
    	\# Amazon Web Services access key id.
    	aws.id = AMAZON_ID
    	
    	\# Amazon Web Services secret key.	
    	aws.secret = AMAZON_SECRET
    	
    	\# Bucket name in s3/s3n.	
    	s3.bucket = BUCKET_NAME
    	
    	\# Name of the vault that the bucket data will be stored in glacier.
    	glacier.vault = VAULT_NAME
    	
    	\# Glacier endpoint i.e. https://glacier.us-east-1.amazonaws.com/
    	glacier.endpoint = GLACIER_ENDPOINT
	
Note, the directory that the data will be archived to is: 
	
	[archivePath]/archive_data/[clusterName]/[serverName]/[indexName]


Run it!
-------

Now feed your index with data and enjoy!



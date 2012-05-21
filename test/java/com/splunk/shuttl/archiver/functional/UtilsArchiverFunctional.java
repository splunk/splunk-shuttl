// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.archiver.functional;

import static com.splunk.shuttl.testutil.UtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.UtilsBucket;
import com.splunk.shuttl.testutil.UtilsEnvironment;
import com.splunk.shuttl.testutil.UtilsTestNG;

/**
 * Util methods for functional archiver tests
 */
public class UtilsArchiverFunctional {
    /**
     * Uses the {@link BucketArchiverFactory#createDefaultArchiver()} to get its
     * {@link PathResolver} and retrieve the URI for the bucket as param.
     * 
     * @return URI to archived bucket in hadoop.
     */
    public static URI getHadoopArchivedBucketURI(Bucket bucket) {
	return getRealPathResolver().resolveArchivePath(bucket);
    }

    /**
     * @return path resolver used with the default archiver.
     */
    public static PathResolver getRealPathResolver() {
	return BucketArchiverFactory.createConfiguredArchiver()
		.getPathResolver();
    }

    /**
     * @return Hadoop {@link FileSystem} configured with shuttl's default host
     *         and port values.
     */
    public static FileSystem getHadoopFileSystem() {
	String hadoopHost = "localhost";
	String hadoopPort = "9000"; // TODO THIS IS NOT OK.!
	try {
	    return FileSystem.get(
		    URI.create("hdfs://" + hadoopHost + ":" + hadoopPort),
		    new Configuration());
	} catch (IOException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Couldn't get Hadoop file system.", e);
	    return null;
	}
    }

    public static void cleanArchivePathInHadoopFileSystem() {
	FileSystem hadoopFileSystem = getHadoopFileSystem();
	Bucket dummyBucket = UtilsBucket.createTestBucket();
	URI hadoopArchivedBucketURI = getHadoopArchivedBucketURI(dummyBucket);
	try {
	    hadoopFileSystem.delete(new Path(hadoopArchivedBucketURI)
		    .getParent().getParent().getParent().getParent()
		    .getParent(), true);
	} catch (IOException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException("Could not clean hadoop file system",
		    e);
	}
    }

    /**
     * 
     */
    public static void waitForAsyncArchiving() {
	try {
	    Thread.sleep(500);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	    UtilsTestNG.failForException(
		    "Got interrupted when waiting for async archiving.", e);
	}
    }

    /**
     * @return
     */
    public static ArchiveConfiguration getLocalCsvArchiveConfigration() {
	String archivePath = createTempDirectory().getAbsolutePath();
	URI archivingRoot = URI.create("file:" + archivePath);
	URI tmpDirectory = URI.create("file:/tmp");
	BucketFormat bucketFormat = BucketFormat.CSV;
	ArchiveConfiguration config = new ArchiveConfiguration(bucketFormat,
		archivingRoot, "clusterName", "serverName",
		asList(bucketFormat), tmpDirectory);
	return config;
    }

    /**
     * Archives bucket given a bucket and a bucketArchiver. Method exists to
     * void duplication between tests that archives buckets.
     */
    public static void archiveBucket(final Bucket bucket,
	    final BucketArchiver bucketArchiver) {
	archiveBucket(bucket, bucketArchiver, "");
    }

    /**
     * Archives bucket given splunkHome, bucket and a bucketArchiver. Method
     * exists to void duplication between tests that archives buckets.
     */
    public static void archiveBucket(final Bucket bucket,
	    final BucketArchiver bucketArchiver, final String splunkHome) {
	UtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		UtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome);
		assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
		bucketArchiver.archiveBucket(bucket);
	    }
	});
    }
}

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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Util methods for functional archiver tests
 */
public class UtilsFunctional {
    /**
     * Uses the {@link BucketArchiverFactory#createDefaultArchiver()} to get its
     * {@link PathResolver} and retrieve the URI for the bucket as param.
     * 
     * @param config
     * 
     * @return URI to archived bucket in hadoop.
     */
    public static URI getHadoopArchivedBucketURI(ArchiveConfiguration config,
	    Bucket bucket) {
	return new PathResolver(config).resolveArchivePath(bucket);
    }

    /**
     * @return path resolver used with the default archiver.
     */
    public static PathResolver getRealPathResolver() {
	return BucketArchiverFactory.createConfiguredArchiver()
		.getPathResolver();
    }

    /**
     * @param hadoopPort2
     * @param hadoopHost2
     * @return Hadoop {@link FileSystem} configured with shuttl's default host
     *         and port values.
     */
    public static FileSystem getHadoopFileSystem(String hadoopHost,
	    String hadoopPort) {
	try {
	    return FileSystem.get(
		    URI.create("hdfs://" + hadoopHost + ":" + hadoopPort),
		    new Configuration());
	} catch (IOException e) {
	    e.printStackTrace();
	    TUtilsTestNG.failForException("Couldn't get Hadoop file system.", e);
	    return null;
	}
    }

    public static void waitForAsyncArchiving() {
	try {
	    Thread.sleep(500);
	} catch (InterruptedException e) {
	    e.printStackTrace();
	    TUtilsTestNG.failForException(
		    "Got interrupted when waiting for async archiving.", e);
	}
    }

    /**
     * @return archive configuration for archiving in a temporary folder on the
     *         local file system with normal {@link BucketFormat.SPLUNK_BUCKET}
     */
    public static ArchiveConfiguration getLocalFileSystemConfiguration() {
	return getLocalFileSystemConfigurationWithFormat(BucketFormat.SPLUNK_BUCKET);
    }

    /**
     * @return configuration configurated for using local file system and CSV
     *         format for buckets.
     */
    public static ArchiveConfiguration getLocalCsvArchiveConfigration() {
	BucketFormat bucketFormat = BucketFormat.CSV;
	return getLocalFileSystemConfigurationWithFormat(bucketFormat);
    }

    private static ArchiveConfiguration getLocalFileSystemConfigurationWithFormat(
	    BucketFormat bucketFormat) {
	String archivePath = createTempDirectory().getAbsolutePath();
	URI archivingRoot = URI.create("file:" + archivePath);
	URI tmpDirectory = URI.create("file:/tmp");
	return new ArchiveConfiguration(bucketFormat, archivingRoot,
		"clusterName", "serverName", asList(bucketFormat), tmpDirectory);
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
	TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

	    @Override
	    public void run() {
		TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME",
			splunkHome);
		assertEquals(BucketFormat.SPLUNK_BUCKET, bucket.getFormat());
		bucketArchiver.archiveBucket(bucket);
	    }
	});
    }

    /**
     * Delete archiving and temp directory of a config that's configured to
     * archive on the local file system.
     */
    public static void tearDownLocalConfig(ArchiveConfiguration config) {
	FileUtils.deleteQuietly(new File(config.getArchivingRoot()));
	FileUtils.deleteQuietly(new File(config.getTmpDirectory()));
    }
}

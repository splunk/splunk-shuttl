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
package com.splunk.shuttl.testutil;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static java.util.Arrays.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Util methods for functional archiver tests
 */
public class TUtilsFunctional {
	/**
	 * @param hadoopPort2
	 * @param hadoopHost2
	 * @return Hadoop {@link FileSystem} configured with shuttl's default host and
	 *         port values.
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
		return getLocalConfigurationThatArchivesFormats(asList(bucketFormat));
	}

	/**
	 * Archives bucket given a bucket and a bucketArchiver. Method exists to void
	 * duplication between tests that archives buckets.
	 */
	public static void archiveBucket(final LocalBucket bucket,
			final BucketArchiver bucketArchiver) {
		archiveBucket(bucket, bucketArchiver, "");
	}

	/**
	 * Archives bucket given splunkHome, bucket and a bucketArchiver. Method
	 * exists to void duplication between tests that archives buckets.
	 */
	public static void archiveBucket(final LocalBucket bucket,
			final BucketArchiver bucketArchiver, final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
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
		FileUtils.deleteQuietly(new File(config.getArchiveDataPath()));
		FileUtils.deleteQuietly(new File(config.getArchiveTempPath()));
	}

	/**
	 * @param bucketFormats
	 *          to archive.
	 * @param hashMap
	 * @return an archive configuration that archives locally (for speed) with all
	 *         the formats specified. The format order in the list also specifies
	 *         the priority when thawing.
	 */
	public static ArchiveConfiguration getLocalConfigurationThatArchivesFormats(
			List<BucketFormat> bucketFormats) {
		return getLocalConfigurationThatArchivesFormats(bucketFormats,
				new HashMap<BucketFormat, Map<String, String>>());
	}

	public static ArchiveConfiguration getLocalConfigurationThatArchivesFormats(
			List<BucketFormat> bucketFormats,
			Map<BucketFormat, Map<String, String>> formatMetadata) {
		String archivePath = createDirectory().getAbsolutePath();
		return ArchiveConfiguration.createSafeConfiguration("localArchiverDir",
				archivePath, bucketFormats, "clusterName", "serverName", bucketFormats,
				"local", formatMetadata);
	}

}

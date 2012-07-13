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
package com.splunk.shuttl.archiver.endtoend;

import static com.splunk.shuttl.testutil.TUtilsFunctional.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.ArchiveRestHandler;
import com.splunk.shuttl.archiver.archive.BucketFreezer;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLocker;
import com.splunk.shuttl.archiver.archive.recovery.BucketMover;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsMockito;
import com.splunk.shuttl.testutil.TUtilsTestNG;

public class ArchiveRecoveryEndToEndTest {

	BucketFreezer failingBucketFreezerWithoutRecovery;
	BucketFreezer successfulBucketFreezerWithRecovery;
	FileSystem hadoopFileSystem;
	ArchiveConfiguration config;
	PathResolver pathResolver;
	private LocalFileSystemPaths localFileSystemPaths;

	@Parameters(value = { "hadoop.host", "hadoop.port" })
	@Test(groups = { "end-to-end" })
	public void Archiver_givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets(
			final String hadoopHost, final String hadoopPort) throws Exception {
		TUtilsMBean.runWithRegisteredMBeans(new Runnable() {

			@Override
			public void run() {
				setUp(hadoopHost, hadoopPort);
				runTests();
				tearDown();
			}

			private void runTests() {
				try {
					givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets();
				} catch (IOException e) {
					TUtilsTestNG.failForException(
							"Got IOException from archive recovery end to end test.", e);
				}
			}
		});
	}

	private void setUp(String hadoopHost, String hadoopPort) {
		config = ArchiveConfiguration.getSharedInstance();
		localFileSystemPaths = LocalFileSystemPaths.create();
		pathResolver = new PathResolver(config);
		hadoopFileSystem = getHadoopFileSystem(hadoopHost, hadoopPort);

		BucketMover bucketMover = new BucketMover(
				localFileSystemPaths.getSafeDirectory());
		BucketLocker bucketLocker = new ArchiveBucketLocker();
		ArchiveRestHandler internalErrorRestHandler = new ArchiveRestHandler(
				TUtilsMockito.createInternalServerErrorHttpClientMock());
		ArchiveRestHandler successfulRealRestHandler = new ArchiveRestHandler(
				new DefaultHttpClient());

		FailedBucketsArchiver noOpFailedBucketsArchiver = mock(FailedBucketsArchiver.class);
		FailedBucketsArchiver realFailedBucketsArchiver = new FailedBucketsArchiver(
				bucketMover, bucketLocker);

		failingBucketFreezerWithoutRecovery = new BucketFreezer(bucketMover,
				bucketLocker, internalErrorRestHandler, noOpFailedBucketsArchiver);

		successfulBucketFreezerWithRecovery = new BucketFreezer(bucketMover,
				bucketLocker, successfulRealRestHandler, realFailedBucketsArchiver);
	}

	private void tearDown() {
		cleanHadoopFileSystem();
		FileUtils.deleteQuietly(localFileSystemPaths.getArchiverDirectory());
	}

	private void cleanHadoopFileSystem() {
		try {
			hadoopFileSystem.delete(new Path(config.getArchivingRoot()), true);
			hadoopFileSystem.delete(new Path(config.getTmpDirectory()), true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private void givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets()
			throws IOException {
		// Setup buckets
		Bucket firstFailingBucket = TUtilsBucket.createBucket();
		Bucket secondFailingBucket = TUtilsBucket.createBucket();
		Bucket successfulBucket = TUtilsBucket.createBucket();

		// Test
		failingBucketFreezerWithoutRecovery.freezeBucket(firstFailingBucket
				.getIndex(), firstFailingBucket.getDirectory().getAbsolutePath());
		failingBucketFreezerWithoutRecovery.freezeBucket(secondFailingBucket
				.getIndex(), secondFailingBucket.getDirectory().getAbsolutePath());

		// Verify bucket archiving failed.
		URI firstBucketURI = pathResolver.resolveArchivePath(firstFailingBucket);
		URI secondBucketURI = pathResolver.resolveArchivePath(secondFailingBucket);
		assertFalse(hadoopFileSystem.exists(new Path(firstBucketURI)));
		assertFalse(hadoopFileSystem.exists(new Path(secondBucketURI)));

		successfulBucketFreezerWithRecovery.freezeBucket(successfulBucket
				.getIndex(), successfulBucket.getDirectory().getAbsolutePath());
		TUtilsFunctional.waitForAsyncArchiving();

		// Verification
		URI thirdBucketURI = pathResolver.resolveArchivePath(successfulBucket);
		assertTrue(hadoopFileSystem.exists(new Path(firstBucketURI)));
		assertTrue(hadoopFileSystem.exists(new Path(secondBucketURI)));
		assertTrue(hadoopFileSystem.exists(new Path(thirdBucketURI)));
	}
}

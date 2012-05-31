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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static com.splunk.shuttl.testutil.TUtilsFunctional.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.ArchiveRestHandler;
import com.splunk.shuttl.archiver.archive.BucketFreezer;
import com.splunk.shuttl.archiver.archive.recovery.BucketLocker;
import com.splunk.shuttl.archiver.archive.recovery.BucketMover;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFunctional;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsMockito;

@Test(groups = { "end-to-end" })
public class ArchiveRecoveryEndToEndTest {

	File safeLocation;
	File originalBucketLocation;

	BucketFreezer failingBucketFreezerWithoutRecovery;
	BucketFreezer successfulBucketFreezerWithRecovery;
	FileSystem hadoopFileSystem;
	private ArchiveConfiguration config;

	@Parameters(value = { "hadoop.host", "hadoop.port" })
	public void Archiver_givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets(
			String hadoopHost, String hadoopPort) throws Exception {
		setUp(hadoopHost, hadoopPort);
		givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets();
		tearDown();
	}

	private void setUp(String hadoopHost, String hadoopPort) {
		TUtilsMBean.registerShuttlArchiverMBean();
		config = ArchiveConfiguration.getSharedInstance();
		hadoopFileSystem = getHadoopFileSystem(hadoopHost, hadoopPort);

		safeLocation = createDirectory();
		originalBucketLocation = createDirectory();

		BucketMover bucketMover = new BucketMover(safeLocation);
		BucketLocker bucketLocker = new BucketLocker();
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

	public void tearDown() {
		FileUtils.deleteQuietly(originalBucketLocation);
		FileUtils.deleteQuietly(safeLocation);
		cleanHadoopFileSystem();
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
			throws IOException, InterruptedException {
		// Setup buckets
		Bucket firstFailingBucket = TUtilsBucket
				.createBucketInDirectory(originalBucketLocation);
		Bucket secondFailingBucket = TUtilsBucket
				.createBucketInDirectory(originalBucketLocation);
		Bucket successfulBucket = TUtilsBucket
				.createBucketInDirectory(originalBucketLocation);

		// Test
		failingBucketFreezerWithoutRecovery.freezeBucket(firstFailingBucket
				.getIndex(), firstFailingBucket.getDirectory().getAbsolutePath());
		failingBucketFreezerWithoutRecovery.freezeBucket(secondFailingBucket
				.getIndex(), secondFailingBucket.getDirectory().getAbsolutePath());

		// Verify bucket archiving failed.
		URI firstBucketURI = TUtilsFunctional.getHadoopArchivedBucketURI(config,
				firstFailingBucket);
		URI secondBucketURI = TUtilsFunctional.getHadoopArchivedBucketURI(config,
				secondFailingBucket);
		assertFalse(hadoopFileSystem.exists(new Path(firstBucketURI)));
		assertFalse(hadoopFileSystem.exists(new Path(secondBucketURI)));

		successfulBucketFreezerWithRecovery.freezeBucket(successfulBucket
				.getIndex(), successfulBucket.getDirectory().getAbsolutePath());
		TUtilsFunctional.waitForAsyncArchiving();

		// Verification
		URI thirdBucketURI = TUtilsFunctional.getHadoopArchivedBucketURI(config,
				successfulBucket);
		assertTrue(hadoopFileSystem.exists(new Path(firstBucketURI)));
		assertTrue(hadoopFileSystem.exists(new Path(secondBucketURI)));
		assertTrue(hadoopFileSystem.exists(new Path(thirdBucketURI)));
	}
}

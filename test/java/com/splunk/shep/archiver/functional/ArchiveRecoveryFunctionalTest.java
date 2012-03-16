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
package com.splunk.shep.archiver.functional;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.ArchiveRestHandler;
import com.splunk.shep.archiver.archive.BucketFreezer;
import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.archive.recovery.BucketLocker;
import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsMockito;

@Test(enabled = false, groups = { "super-slow" })
public class ArchiveRecoveryFunctionalTest {

    File safeLocation;
    File originalBucketLocation;

    BucketFreezer failingBucketFreezerWithoutRecovery;
    BucketFreezer successfulBucketFreezerWithRecovery;
    FileSystem hadoopFileSystem;

    @BeforeMethod
    public void setUp() {
	safeLocation = createTempDirectory();
	originalBucketLocation = createTempDirectory();

	BucketMover bucketMover = new BucketMover(
		safeLocation.getAbsolutePath());
	BucketLocker bucketLocker = new BucketLocker();
	ArchiveRestHandler internalErrorRestHandler = new ArchiveRestHandler(
		UtilsMockito.createInternalServerErrorHttpClientMock());
	ArchiveRestHandler successfulRealRestHandler = new ArchiveRestHandler(
		new DefaultHttpClient());

	FailedBucketsArchiver noOpFailedBucketsArchiver = mock(FailedBucketsArchiver.class);
	FailedBucketsArchiver realFailedBucketsArchiver = new FailedBucketsArchiver(
		bucketMover, bucketLocker);

	failingBucketFreezerWithoutRecovery = new BucketFreezer(bucketMover,
		bucketLocker, internalErrorRestHandler,
		noOpFailedBucketsArchiver);

	successfulBucketFreezerWithRecovery = new BucketFreezer(bucketMover,
		bucketLocker, successfulRealRestHandler,
		realFailedBucketsArchiver);

	hadoopFileSystem = UtilsArchiverFunctional.getHadoopFileSystem();
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(safeLocation);
	FileUtils.deleteDirectory(originalBucketLocation);
	FileUtils.deleteDirectory(new File(BucketLock.DEFAULT_LOCKS_DIRECTORY));
    }

    public void Archiver_givenTwoFailedBucketAttempts_archivesTheThirdBucketAndTheTwoFailedBuckets()
	    throws IOException, InterruptedException {
	// Setup buckets
	try {
	    Bucket firstFailingBucket = UtilsBucket
		    .createBucketInDirectory(originalBucketLocation);
	    Bucket secondFailingBucket = UtilsBucket
		    .createBucketInDirectory(originalBucketLocation);
	    Bucket successfulBucket = UtilsBucket
		    .createBucketInDirectory(originalBucketLocation);

	    // Test
	    failingBucketFreezerWithoutRecovery.freezeBucket(firstFailingBucket
		    .getIndex(), firstFailingBucket.getDirectory()
		    .getAbsolutePath());
	    failingBucketFreezerWithoutRecovery.freezeBucket(
		    secondFailingBucket.getIndex(), secondFailingBucket
			    .getDirectory().getAbsolutePath());

	    // Verify bucket archiving failed.
	    URI firstBucketURI = UtilsArchiverFunctional
		    .getHadoopArchivedBucketURI(firstFailingBucket);
	    URI secondBucketURI = UtilsArchiverFunctional
		    .getHadoopArchivedBucketURI(secondFailingBucket);
	    assertFalse(hadoopFileSystem.exists(new Path(firstBucketURI)));
	    assertFalse(hadoopFileSystem.exists(new Path(secondBucketURI)));

	    successfulBucketFreezerWithRecovery.freezeBucket(successfulBucket
		    .getIndex(), successfulBucket.getDirectory()
		    .getAbsolutePath());
	    UtilsArchiverFunctional.waitForAsyncArchiving();

	    // Verification
	    URI thirdBucketURI = UtilsArchiverFunctional
		    .getHadoopArchivedBucketURI(successfulBucket);
	    assertTrue(hadoopFileSystem.exists(new Path(firstBucketURI)));
	    assertTrue(hadoopFileSystem.exists(new Path(secondBucketURI)));
	    assertTrue(hadoopFileSystem.exists(new Path(thirdBucketURI)));
	} finally {
	    UtilsArchiverFunctional.cleanArchivePathInHadoopFileSystem();
	}
    }
}

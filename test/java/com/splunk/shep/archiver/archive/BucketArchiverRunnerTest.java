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
package com.splunk.shep.archiver.archive;

import static org.mockito.Mockito.*;

import java.io.IOException;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.archive.recovery.SimpleFileLock.NotLockedException;
import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverRunnerTest {

    BucketArchiverRunner bucketArchiverRunner;
    BucketArchiver bucketArchiver;
    Bucket bucket;
    BucketLock bucketLock;

    @BeforeMethod
    public void setUp() {
	bucketArchiver = mock(BucketArchiver.class);
	bucket = mock(Bucket.class);
	bucketLock = mock(BucketLock.class);
	when(bucketLock.isLocked()).thenReturn(true);
	bucketArchiverRunner = new BucketArchiverRunner(bucketArchiver, bucket,
		bucketLock);
    }

    @Test(expectedExceptions = { NotLockedException.class })
    public void constructor_bucketLockIsNotLocked_throwNotLockedException() {
	BucketLock bucketLock = mock(BucketLock.class);
	when(bucketLock.isLocked()).thenReturn(false);
	new BucketArchiverRunner(bucketArchiver, bucket, bucketLock);
    }

    @Test(groups = { "fast-unit" })
    public void run_givenSetUp_deletesBucketAfterSuccessfulArchiving()
	    throws IOException {
	bucketArchiverRunner.run();
	InOrder inOrder = inOrder(bucketArchiver, bucket);
	inOrder.verify(bucketArchiver).archiveBucket(bucket);
	inOrder.verify(bucket).deleteBucket();
    }

    public void run_successfulArchiving_closesAndDeletesBucketLock() {
	bucketArchiverRunner.run();
	verify(bucketLock).closeLock();
	verify(bucketLock).deleteLockFile();
    }

    public void run_failedArchiving_closesAndDeletesBucketLock() {
	doThrow(new RuntimeException()).when(bucketArchiver).archiveBucket(
		bucket);
	try {
	    bucketArchiverRunner.run();
	} catch (RuntimeException re) {
	    // Do nothing.
	}
	verify(bucketLock).closeLock();
	verify(bucketLock).deleteLockFile();
    }

}

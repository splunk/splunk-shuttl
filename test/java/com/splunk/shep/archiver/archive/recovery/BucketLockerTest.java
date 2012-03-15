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
package com.splunk.shep.archiver.archive.recovery;

import static com.splunk.shep.testutil.UtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.mockito.InOrder;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class BucketLockerTest {

    File tempTestDirectory;
    Bucket bucket;
    BucketLocker bucketLocker;

    @BeforeMethod
    public void setUp() {
	tempTestDirectory = createTempDirectory();
	bucket = UtilsBucket.createBucketInDirectory(tempTestDirectory);
	bucketLocker = new BucketLocker();
    }

    @AfterMethod
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(tempTestDirectory);
	FileUtils.deleteDirectory(new File(BucketLock.DEFAULT_LOCKS_DIRECTORY));
    }

    public void runWithBucketLocked_givenBucketThatCanBeLocked_executesRunnable() {
	final Boolean[] isRun = new Boolean[] { false };
	bucketLocker.runWithBucketLocked(bucket, new Runnable() {

	    @Override
	    public void run() {
		isRun[0] = true;
	    }

	});
	assertTrue(isRun[0]);
    }

    public void runWithBucketLocked_givenLockedBucket_doesNotExecuteRunnable() {
	BucketLock bucketLock = new BucketLock(bucket);
	assertTrue(bucketLock.tryLock());
	bucketLocker.runWithBucketLocked(bucket, new Runnable() {

	    @Override
	    public void run() {
		fail();
	    }
	});
    }

    public void runWithBucketLocked_runOnceAlreadyAndReleasedTheLock_executesRunnable() {
	final Boolean[] isRun = new Boolean[] { false };
	bucketLocker.runWithBucketLocked(bucket, new NoOpRunnable());
	bucketLocker.runWithBucketLocked(bucket, new Runnable() {

	    @Override
	    public void run() {
		isRun[0] = true;
	    }
	});
	assertTrue(isRun[0]);
    }

    private static class NoOpRunnable implements Runnable {
	@Override
	public void run() {
	    // Do nothing.
	}
    }

    public void executeRunnableDuringBucketLock_givenTryLockThatReturnsFalse_stillClosesLock() {
	BucketLock bucketLock = mock(BucketLock.class);
	when(bucketLock.tryLock()).thenReturn(false);
	bucketLocker.executeRunnableDuringBucketLock(bucketLock,
		new NoOpRunnable());
	InOrder inOrder = inOrder(bucketLock);
	inOrder.verify(bucketLock).tryLock();
	inOrder.verify(bucketLock).closeLock();
	inOrder.verifyNoMoreInteractions();
    }

}

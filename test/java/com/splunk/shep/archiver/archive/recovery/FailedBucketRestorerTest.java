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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import java.util.Arrays;
import java.util.List;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class FailedBucketRestorerTest {

    FailedBucketRestorer failedBucketRestorer;
    FailedBucketRecoveryHandler bucketRecoveryHandler;
    BucketMover bucketMover;
    FailedBucketLock lock;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	bucketMover = mock(BucketMover.class);
	bucketRecoveryHandler = mock(FailedBucketRecoveryHandler.class);
	lock = mock(FailedBucketLock.class);
	failedBucketRestorer = new FailedBucketRestorer(bucketMover,
		lock);
    }

    private Bucket stubOneBucketInFailedBucketTransfers() {
	Bucket failedBucket = UtilsBucket.createTestBucket();
	List<Bucket> failedBuckets = Arrays.asList(failedBucket);

	when(bucketMover.getFailedBuckets())
		.thenReturn(failedBuckets);
	return failedBucket;
    }

    public void recoverFailedBuckets_gotLockAndThereIsOneFailedBucket_letBucketHandlerRecoverFailedBucket() {
	Bucket failedBucket = stubOneBucketInFailedBucketTransfers();
	when(lock.tryLock()).thenReturn(true);
	failedBucketRestorer.recoverFailedBuckets(bucketRecoveryHandler);

	verify(bucketRecoveryHandler).recoverFailedBucket(failedBucket);
    }

    public void recoverFailedBuckets_cantGetFailedBucketLockAndThereIsOneFailedBucket_bucketRecoveryHandlerIsNotCalled() {
	stubOneBucketInFailedBucketTransfers();
	when(lock.tryLock()).thenReturn(false);

	failedBucketRestorer.recoverFailedBuckets(bucketRecoveryHandler);

	verify(bucketRecoveryHandler, times(0)).recoverFailedBucket(
		any(Bucket.class));
    }

    public void redoverFailedBuckets_anyState_tryLockThenCloseLock() {
	failedBucketRestorer.recoverFailedBuckets(bucketRecoveryHandler);
	InOrder inOrder = inOrder(lock);
	inOrder.verify(lock).tryLock();
	inOrder.verify(lock).closeLock();
	verifyNoMoreInteractions(lock);
    }
}

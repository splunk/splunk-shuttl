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
package com.splunk.shuttl.archiver.bucketlock;

import static org.mockito.Mockito.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketlock.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.bucketlock.BucketLockerTest.NoOpBucketHandler;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Fixture: BucketLock mock to verify that {@link BucketLock#closeLock()} is
 * called.
 */
@Test(groups = { "fast-unit" })
public class BucketLockerCloseLockTest {

	BucketLocker bucketLocker;
	BucketLock bucketLock;

	@BeforeMethod
	public void setUp() {
		bucketLock = mock(BucketLock.class);
		bucketLocker = new BucketLocker() {

			@Override
			public BucketLock getLockForBucket(Bucket bucket) {
				return bucketLock;
			}
		};
	}

	@Test(groups = { "fast-unit" })
	public void callBucketHandlerWithBucketSharedLock_givenTryLockThatReturnsFalse_closesLock() {
		when(bucketLock.tryLockExclusive()).thenReturn(false);
		callBucketLocker();
		InOrder inOrder = inOrder(bucketLock);
		inOrder.verify(bucketLock).tryLockExclusive();
		inOrder.verify(bucketLock).closeLock();
	}

	private void callBucketLocker() {
		bucketLocker.callBucketHandlerUnderSharedLock(mock(Bucket.class),
				new NoOpBucketHandler());
	}

	public void callBucketHandlerWithBucketSharedLock_givenSharedConvertFail_closesLock() {
		when(bucketLock.tryLockExclusive()).thenReturn(true);
		when(bucketLock.tryConvertExclusiveToSharedLock()).thenReturn(false);
		callBucketLocker();
		verify(bucketLock).closeLock();
	}

	public void callBucketHandlerWithBucketSharedLock_givenSuccessfulSharedLock_closesLock() {
		when(bucketLock.tryLockExclusive()).thenReturn(true);
		when(bucketLock.tryConvertExclusiveToSharedLock()).thenReturn(true);
		callBucketLocker();
		verify(bucketLock).closeLock();
	}

	public void callBucketHandlerWithBucketSharedLock_givenRunnableThatThrowsException_closesLock() {
		when(bucketLock.tryLockExclusive()).thenReturn(true);
		when(bucketLock.tryConvertExclusiveToSharedLock()).thenReturn(true);
		try {
			bucketLocker.callBucketHandlerUnderSharedLock(mock(Bucket.class),
					new SharedLockBucketHandler() {
						@Override
						public void handleSharedLockedBucket(Bucket bucket) {
							throw new FakeException();
						}

						@Override
						public void bucketWasLocked(Bucket bucket) {
						}
					});
		} catch (FakeException fake) {
			// Catch runnables exception to see if it still closes lock in case
			// of any exception.
		}
		verify(bucketLock).closeLock();
	}

	@SuppressWarnings("serial")
	private static class FakeException extends RuntimeException {
	}

}

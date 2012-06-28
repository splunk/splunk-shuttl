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
package com.splunk.shuttl.archiver.archive.recovery;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.recovery.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class FailedBucketsArchiverTest {

	FailedBucketsArchiver failedBucketsArchiver;
	SharedLockBucketHandler sharedLockBucketHandler;
	BucketMover bucketMover;
	BucketLocker bucketLocker;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		bucketMover = mock(BucketMover.class);
		bucketLocker = new BucketLockerInTestDir(createDirectory());
		sharedLockBucketHandler = mock(SharedLockBucketHandler.class);
		failedBucketsArchiver = new FailedBucketsArchiver(bucketMover, bucketLocker);
	}

	private List<Bucket> stubXBucketsInBucketMover(int x) {
		List<Bucket> buckets = new ArrayList<Bucket>();
		for (int i = 0; i < x; i++) {
			Bucket movedBucket = TUtilsBucket.createBucket();
			buckets.add(movedBucket);
		}
		when(bucketMover.getMovedBuckets()).thenReturn(buckets);
		return buckets;
	}

	@Test(groups = { "fast-unit" })
	public void archiveFailedBuckets_noMovedBuckets_neverRunLockedBucketHandler() {
		failedBucketsArchiver.archiveFailedBuckets(sharedLockBucketHandler);
		verify(sharedLockBucketHandler, times(0)).handleSharedLockedBucket(
				any(Bucket.class));
	}

	public void archiveFailedBuckets_thereIsOneMovedBucket_letBucketLockerRunLockedBucketHandlerOnIt() {
		List<Bucket> bucketsInBucketMover = stubXBucketsInBucketMover(1);
		assertEquals(1, bucketsInBucketMover.size());
		Bucket bucket = bucketsInBucketMover.get(0);
		failedBucketsArchiver.archiveFailedBuckets(sharedLockBucketHandler);

		verify(sharedLockBucketHandler, times(1)).handleSharedLockedBucket(bucket);
	}

	public void archiveFailedBuckets_twoMovedBuckets_runLockedBucketHandlerTwice() {
		List<Bucket> bucketsInBucketMover = stubXBucketsInBucketMover(2);
		assertEquals(2, bucketsInBucketMover.size());
		failedBucketsArchiver.archiveFailedBuckets(sharedLockBucketHandler);
		verify(sharedLockBucketHandler, times(2)).handleSharedLockedBucket(
				any(Bucket.class));
	}
}

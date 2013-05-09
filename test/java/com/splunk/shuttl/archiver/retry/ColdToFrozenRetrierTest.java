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
package com.splunk.shuttl.archiver.retry;

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.archive.recovery.IndexPreservingBucketMover;
import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsTestNG;

@Test(groups = { "slow-unit" })
public class ColdToFrozenRetrierTest {

	private ColdToFrozenRetrier retrier;
	private IndexPreservingBucketMover bucketMover;

	private File safeBucketDirectory;
	private AddsFileToBucket addsFileToBucket;
	private BucketLocker bucketLocker;

	@BeforeMethod
	public void setUp() {
		safeBucketDirectory = createDirectory();
		bucketMover = IndexPreservingBucketMover.create(safeBucketDirectory);
		bucketLocker = new BucketLocker() {

			@Override
			public BucketLock getLockForBucket(Bucket bucket) {
				return new BucketLock(bucket, createDirectory());
			}
		};
		FailedBucketsArchiver failedBucketsArchiver = new FailedBucketsArchiver(
				bucketMover, bucketLocker);

		addsFileToBucket = new AddsFileToBucket();
		retrier = new ColdToFrozenRetrier(failedBucketsArchiver,
				addsFileToBucket);
	}

	private static class AddsFileToBucket implements SharedLockBucketHandler {

		@Override
		public void handleSharedLockedBucket(Bucket bucket) {
			try {
				getFileToAdd((LocalBucket) bucket).createNewFile();
			} catch (IOException e) {
				TUtilsTestNG.failForException(null, e);
			}
		}

		public File getFileToAdd(LocalBucket bucket) {
			return new File((bucket).getDirectory(), "foo.bar");
		}

		@Override
		public void bucketWasLocked(Bucket bucket) {
		}
	}

	@AfterMethod
	public void tearDown() {
		FileUtils.deleteQuietly(safeBucketDirectory);
	}

	@Test(timeOut = 3000)
	public void _verifyingRightAfterCallingRun_moveAndFileAddingHasNotHappenedYet()
			throws InterruptedException {
		LocalBucket bucket = TUtilsBucket.createBucket();
		bucketMover.moveBucket(bucket);

		Thread thread = startRetrier();
		assertFalse(wasBucketMovedAndFileAdded(bucket));
		stopRetrier(thread);
		assertTrue(wasBucketMovedAndFileAdded(bucket));
	}

	private void stopRetrier(Thread thread) throws InterruptedException {
		stopRetrier(thread, retrier);
	}

	private void stopRetrier(Thread thread, ColdToFrozenRetrier retrier)
			throws InterruptedException {
		thread.join();
	}

	private Thread startRetrier() {
		return startRetrier(retrier);
	}

	private Thread startRetrier(ColdToFrozenRetrier retrier) {
		Thread thread = new Thread(retrier);
		thread.start();
		return thread;
	}

	private boolean wasBucketMovedAndFileAdded(LocalBucket bucket) {
		LocalBucket movedBucket = getMovedBucket(bucket);
		return !bucket.getDirectory().exists()
				&& addsFileToBucket.getFileToAdd(movedBucket).exists();
	}

	private LocalBucket getMovedBucket(LocalBucket failedBucket) {
		LocalBucket movedBucket = (LocalBucket) bucketMover.getMovedBuckets()
				.get(0);
		TUtilsTestNG.assertBucketsGotSameIndexFormatAndName(failedBucket,
				movedBucket);
		return movedBucket;
	}

	public void _anotherBucketAfterTheFirstOneHasBeenMoved_retriesBothBuckets()
			throws InterruptedException {
		LocalBucket first = TUtilsBucket.createBucket();
		LocalBucket second = TUtilsBucket.createBucket();

		bucketMover.moveBucket(first);
		bucketMover.moveBucket(second);

		retrier.run();

		for (Bucket b : bucketMover.getMovedBuckets())
			assertTrue(b.getName().equals(first.getName())
					|| b.getName().equals(second.getName()));
	}

	public void _startingThenStoppingTheRetrier_doesNotRetryBucketsAfterHaveBeingStopped()
			throws InterruptedException {
		Thread thread = startRetrier();
		stopRetrier(thread);

		LocalBucket bucket = TUtilsBucket.createBucket();
		bucketMover.moveBucket(bucket);
		assertFalse(wasBucketMovedAndFileAdded(bucket));
	}

}

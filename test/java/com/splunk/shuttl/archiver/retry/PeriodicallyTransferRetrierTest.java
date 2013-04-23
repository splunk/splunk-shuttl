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
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

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
public class PeriodicallyTransferRetrierTest {

	private PeriodicallyTransferRetrier retrier;
	private long retryTime;
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

		retryTime = 15;
		addsFileToBucket = new AddsFileToBucket();
		retrier = new PeriodicallyTransferRetrier(failedBucketsArchiver,
				addsFileToBucket, retryTime);
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

	private void stopRetrier(Thread thread, PeriodicallyTransferRetrier retrier)
			throws InterruptedException {
		retrier.stop();
		thread.join();
	}

	private Thread startRetrier() {
		return startRetrier(retrier);
	}

	private Thread startRetrier(PeriodicallyTransferRetrier retrier) {
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

	@Test(timeOut = 3000)
	public void _anotherBucketAfterTheFirstOneHasBeenMoved_retriesBothBuckets()
			throws InterruptedException {
		LocalBucket first = TUtilsBucket.createBucket();
		LocalBucket second = TUtilsBucket.createBucket();

		bucketMover.moveBucket(first);

		Thread thread = startRetrier();
		waitUntilRetrierIsDoneWorking();
		assertTrue(wasBucketMovedAndFileAdded(first));
		deleteMovedBuckets();

		bucketMover.moveBucket(second);
		waitUntilRetrierIsDoneWorking();

		stopRetrier(thread);
		assertTrue(wasBucketMovedAndFileAdded(second));
	}

	private void waitUntilRetrierIsDoneWorking() throws InterruptedException {
		int counter = 0;
		long timeout = 1000;
		while (!fileHasBeenAddedToAllMovedBuckets())
			if (++counter > timeout)
				throw new RuntimeException(
						"Waited too long for retrier to do its work.");
			else
				Thread.sleep(1);
	}

	private boolean fileHasBeenAddedToAllMovedBuckets() {
		List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
		if (movedBuckets.isEmpty())
			return false;
		for (Bucket b : movedBuckets)
			if (!addsFileToBucket.getFileToAdd((LocalBucket) b).exists())
				return false;
		return true;
	}

	private void deleteMovedBuckets() {
		for (Bucket b : bucketMover.getMovedBuckets())
			FileUtils.deleteQuietly(((LocalBucket) b).getDirectory());
	}

	public void _startingThenStoppingTheRetrier_doesNotRetryBucketsAfterHaveBeingStopped()
			throws InterruptedException {
		Thread thread = startRetrier();
		stopRetrier(thread);

		LocalBucket bucket = TUtilsBucket.createBucket();
		bucketMover.moveBucket(bucket);
		assertFalse(wasBucketMovedAndFileAdded(bucket));
	}

	@Test(timeOut = 2000)
	public void isRunning__trueWhenRetrierIsRunning() throws InterruptedException {
		assertFalse(retrier.isRunning());
		Thread thread = startRetrier();
		waitForRetierToStartRunning();
		stopRetrier(thread);
		assertFalse(retrier.isRunning());
	}

	private void waitForRetierToStartRunning() {
		waitForRetierToStartRunning(retrier);
	}

	private void waitForRetierToStartRunning(PeriodicallyTransferRetrier retrier) {
		while (!retrier.isRunning())
			Thread.yield();
	}

	@Test(timeOut = 2000)
	public void _failedBucketArchiverThrowingException_stillContinuesToRetry()
			throws InterruptedException {
		FailedBucketsArchiver archiver = mock(FailedBucketsArchiver.class);
		PeriodicallyTransferRetrier retrier = new PeriodicallyTransferRetrier(
				archiver, null, 1);
		doThrow(new RuntimeException()).when(archiver).archiveFailedBuckets(null);

		Thread thread = startRetrier(retrier);
		waitForRetierToStartRunning(retrier);
		verify(archiver, atLeastOnce()).archiveFailedBuckets(null);
		assertTrue(retrier.isRunning());
		stopRetrier(thread, retrier);
	}
}

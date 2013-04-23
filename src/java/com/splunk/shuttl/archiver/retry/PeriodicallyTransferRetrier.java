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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveRestHandler;
import com.splunk.shuttl.archiver.archive.RegistersArchiverMBean;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLocker;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.archive.recovery.IndexPreservingBucketMover;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker.SharedLockBucketHandler;

/**
 * Periodically retries to transfer buckets moved by the bucket mover.
 */
public class PeriodicallyTransferRetrier implements Runnable {

	private static final Logger logger = Logger
			.getLogger(PeriodicallyTransferRetrier.class);

	private final FailedBucketsArchiver failedBucketsArchiver;
	private final SharedLockBucketHandler sharedLockBucketHandler;
	private final long retryTimeInMs;

	private boolean stop = false;
	private boolean isRunning = false;

	public PeriodicallyTransferRetrier(
			FailedBucketsArchiver failedBucketsArchiver,
			SharedLockBucketHandler sharedLockBucketHandler, long retryTimeInMs) {
		this.failedBucketsArchiver = failedBucketsArchiver;
		this.sharedLockBucketHandler = sharedLockBucketHandler;
		this.retryTimeInMs = retryTimeInMs;
	}

	@Override
	public void run() {
		isRunning = true;
		try {
			runLoop();
		} finally {
			isRunning = false;
		}
	}

	private void runLoop() {
		while (!stop && !Thread.currentThread().isInterrupted()) {
			retryTransferBuckets();
			waitAWhile();
		}
	}

	private void retryTransferBuckets() {
		try {
			failedBucketsArchiver.archiveFailedBuckets(sharedLockBucketHandler);
		} catch (Exception e) {
			logger.info(did("Get an exception when retrying to archive buckets", e,
					"that the retry of a failed archived bucket might work", "exception",
					e));
		}
	}

	private void waitAWhile() {
		try {
			Thread.sleep(retryTimeInMs);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Stops the retrier
	 */
	public void stop() {
		stop = true;
	}

	public boolean isRunning() {
		return isRunning;
	}

	public static void main(String[] args) throws InterruptedException {
		long retryEveryMinute = 1000 * 60;

		PeriodicallyTransferRetrier retrier = createRetrier(retryEveryMinute);

		Thread thread = new Thread(retrier);
		thread.start();
		thread.join();
	}

	private static PeriodicallyTransferRetrier createRetrier(long retryTimeInMs) {
		RegistersArchiverMBean.create().register();

		IndexPreservingBucketMover bucketMover = IndexPreservingBucketMover
				.create(LocalFileSystemPaths.create().getSafeDirectory());
		BucketLocker bucketLocker = new ArchiveBucketLocker();
		FailedBucketsArchiver failedBucketsArchiver = new FailedBucketsArchiver(
				bucketMover, bucketLocker);
		ArchiveRestHandler archiveRestHandler = ArchiveRestHandler.create();

		return new PeriodicallyTransferRetrier(failedBucketsArchiver,
				archiveRestHandler, retryTimeInMs);
	}
}

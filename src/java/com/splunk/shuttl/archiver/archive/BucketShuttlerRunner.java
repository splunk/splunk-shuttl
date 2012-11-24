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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.bucketlock.SimpleFileLock.NotLockedException;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * The {@link BucketShuttlerRunner} makes sure that the bucket is locked,
 * archived, deleted after successful archiving, unlocked and deletion of lock
 * file.
 */
public class BucketShuttlerRunner implements Runnable {

	private final static Logger logger = Logger
			.getLogger(BucketShuttlerRunner.class);

	private final BucketShuttler bucketShuttler;
	private final LocalBucket bucket;
	private final BucketLock bucketLock;

	/**
	 * @param bucketShuttler
	 *          for shuttling a bucket.
	 * @param bucket
	 *          to shuttl.
	 * @param bucketLock
	 *          which is already locked.
	 */
	public BucketShuttlerRunner(BucketShuttler bucketShuttler,
			LocalBucket bucket, BucketLock bucketLock) {
		if (!bucketLock.isLocked())
			throw new NotLockedException("Bucket Lock has to be locked already"
					+ " before archiving bucket");
		this.bucketShuttler = bucketShuttler;
		this.bucket = bucket;
		this.bucketLock = bucketLock;
	}

	@Override
	public void run() {
		if (bucketLock.isLocked())
			archiveBucket();
		else
			handleErrorThatBucketShouldStillBeLocked();
	}

	private void handleErrorThatBucketShouldStillBeLocked() {
		logger.debug(did("Was going to archive bucket",
				"Bucket was not locked before archiving it",
				"Bucket must have been locked before archiving, "
						+ "to make sure that it is safe to transfer "
						+ "the bucket to archive without " + "any other modification.",
				"bucket", bucket));
		throw new IllegalStateException("Bucket should still be locked"
				+ " when starting to archive " + "the bucket. Aborting archiving.");
	}

	private void archiveBucket() {
		try {
			logger.info(will("Archiving bucket", "bucket", bucket));
			bucketShuttler.shuttlBucket(bucket);
			logger.info(done("Archived bucket", "bucket", bucket));
		} finally {
			bucketLock.deleteLockFile();
			bucketLock.closeLock();
		}
	}
}

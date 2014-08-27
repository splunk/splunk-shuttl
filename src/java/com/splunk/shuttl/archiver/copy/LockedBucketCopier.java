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
package com.splunk.shuttl.archiver.copy;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Controls what happens when copying a bucket.
 */
public class LockedBucketCopier {

	private static Logger logger = Logger.getLogger(LockedBucketCopier.class);

	private final CallCopyBucketEndpoint endpoint;
	private final CopyBucketReceipts receipts;
	private BucketLocker bucketLocker;

	public LockedBucketCopier(BucketLocker bucketLocker,
			CallCopyBucketEndpoint endpoint, CopyBucketReceipts receipts) {
		this.bucketLocker = bucketLocker;
		this.endpoint = endpoint;
		this.receipts = receipts;
	}

	/**
	 * Locks and copies a bucket. Then copies all the buckets that has not
	 * successfully been copied.
	 */
	public void copyBucket(LocalBucket bucket) {
		bucketLocker.callBucketHandlerUnderSharedLock(bucket,
				new CopyBucketUnderLock());
	}

	private class CopyBucketUnderLock implements SharedLockBucketHandler {

		@Override
		public void handleSharedLockedBucket(Bucket bucket) {
			logger.debug(will("call copy bucket endpoint", "bucket", bucket));
			LocalBucket localBucket = (LocalBucket) bucket;
			try {
				endpoint.call(localBucket);
				logger.debug(done("calling copy bucket endpoint", "bucket", bucket));
				receipts.createReceipt(localBucket);
			} catch (RuntimeException e) {
				logger.error(did("Call copy endpoint to copy bucket", e,
						"to copy and then create a copy receipt", "bucket", bucket));
			}
		}

		@Override
		public void bucketWasLocked(Bucket bucket) {

		}

	}
}

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

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Class for locking buckets, synchronizing usages of buckets by locking a
 * bucket before using/modifying it.
 */
public abstract class BucketLocker {

	/**
	 * LockedBucketHandler is not executed if the bucket cannot be locked
	 * exclusively first, and then converting the lock to being shared.
	 */
	public void callBucketHandlerUnderSharedLock(Bucket bucket,
			SharedLockBucketHandler bucketHandler) {
		callBucketHandlerWithBucketSharedLock(getLockForBucket(bucket), bucket,
				bucketHandler);
	}

	/**
	 * @return {@link BucketLock} instance for bucket, which knows where the
	 *         buckets are stored.
	 */
	protected abstract BucketLock getLockForBucket(Bucket bucket);

	/**
	 * Method exists for verifying that {@link BucketLock} is closed, whether it
	 * gets the lock or not.
	 */
	private void callBucketHandlerWithBucketSharedLock(
			BucketLock bucketLock, Bucket bucket,
			SharedLockBucketHandler bucketHandler) {
		try {
			executeBucketHandlerIfSharedLockIsAcquired(bucketLock, bucket,
					bucketHandler);
		} finally {
			bucketLock.closeLock();
		}
	}

	private void executeBucketHandlerIfSharedLockIsAcquired(
			BucketLock bucketLock, Bucket bucket,
			SharedLockBucketHandler bucketHandler) {
		if (bucketLock.tryLockExclusive())
			if (bucketLock.tryConvertExclusiveToSharedLock())
				bucketHandler.handleSharedLockedBucket(bucket);
	}

	/**
	 * Interface for operating on a {@link Bucket} while it's locked with
	 * {@link BucketLock}.
	 */
	public interface SharedLockBucketHandler {

		/**
		 * Do operations on a {@link Bucket} while it's locked with a
		 * {@link BucketLock}.
		 */
		void handleSharedLockedBucket(Bucket bucket);
	}

}

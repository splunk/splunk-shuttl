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

import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Controls what happens when copying a bucket.
 */
public class LockedBucketCopier {

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
		if (acquiredLockForBucket(bucket))
			doCopyBucket(bucket);
	}

	private boolean acquiredLockForBucket(LocalBucket bucket) {
		return bucketLocker.getLockForBucket(bucket).isLocked();
	}

	private void doCopyBucket(LocalBucket bucket) {
		endpoint.call(bucket);
		receipts.createReceipt(bucket);
	}

}

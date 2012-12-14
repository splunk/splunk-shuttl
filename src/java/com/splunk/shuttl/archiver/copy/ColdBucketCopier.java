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

import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Copies buckets that has not already been copied that lies in the cold
 * directory of a bucket that was just copied.
 */
public class ColdBucketCopier {

	private final ColdBucketInterator bucketInterator;
	private final CopyBucketReceipts receipts;
	private final LockedBucketCopier lockedCopier;

	public ColdBucketCopier(ColdBucketInterator bucketInterator,
			CopyBucketReceipts receipts, LockedBucketCopier lockedCopier) {
		this.bucketInterator = bucketInterator;
		this.receipts = receipts;
		this.lockedCopier = lockedCopier;
	}

	/**
	 * Copies buckets that is in the cold directory that hasn't been copied
	 * already.
	 */
	public void tryCopyingColdBuckets(String index) {
		for (LocalBucket b : bucketInterator.coldBucketsAtIndex(index))
			copyColdBucket(b);
	}

	void copyColdBucket(LocalBucket b) {
		if (!receipts.hasReceipt(b))
			lockedCopier.copyBucket(b);
	}
}

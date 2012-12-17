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

import static java.util.Arrays.*;
import static org.mockito.Mockito.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ColdBucketCopierTest {

	private ColdBucketCopier coldBucketCopier;
	private ColdBucketInterator bucketInterator;
	private CopyBucketReceipts receipts;
	private LockedBucketCopier lockedCopier;

	private LocalBucket localBucket;

	@BeforeMethod
	public void setUp() {
		bucketInterator = mock(ColdBucketInterator.class);
		receipts = mock(CopyBucketReceipts.class);
		lockedCopier = mock(LockedBucketCopier.class);

		coldBucketCopier = new ColdBucketCopier(bucketInterator, receipts,
				lockedCopier);

		localBucket = mock(LocalBucket.class);
	}

	public void tryCopyingColdBuckets_givenIndex_checksReceiptOnColdBuckets() {
		when(bucketInterator.coldBucketsAtIndex("index")).thenReturn(
				asList(localBucket));
		coldBucketCopier.tryCopyingColdBuckets("index");
		verify(receipts).hasReceipt(localBucket);
	}

	public void copyColdBucket_givenBucketWithoutReceipt_callsLockedCopier() {
		when(receipts.hasReceipt(localBucket)).thenReturn(false);
		coldBucketCopier.copyColdBucket(localBucket);
		verify(lockedCopier).copyBucket(localBucket);
	}

	public void copyColdBucket_givenBucketWithReceipt_doesNotCopyBucket() {
		when(receipts.hasReceipt(localBucket)).thenReturn(true);
		coldBucketCopier.copyColdBucket(localBucket);
		verifyZeroInteractions(lockedCopier);
	}

	public void copyColdBucket_givenReplicatedBucket_doesNotCopyBucket() {
		LocalBucket replicatedBucket = TUtilsBucket.createReplicatedBucket();
		coldBucketCopier.copyColdBucket(replicatedBucket);
		verifyZeroInteractions(lockedCopier);
	}
}

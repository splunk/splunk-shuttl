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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class LockedBucketCopierTest {

	private LockedBucketCopier copyBucketEntryPoint;
	private CallCopyBucketEndpoint endpoint;
	private CopyBucketReceipts receipts;
	private BucketLocker realBucketLocker;
	private LocalBucket bucket;

	@BeforeMethod
	public void setUp() {
		realBucketLocker = new CopyBucketLocker(new LocalFileSystemPaths(
				createDirectory()));
		endpoint = mock(CallCopyBucketEndpoint.class);
		receipts = mock(CopyBucketReceipts.class);
		copyBucketEntryPoint = new LockedBucketCopier(realBucketLocker, endpoint,
				receipts);

		bucket = TUtilsBucket.createBucket();
	}

	public void copyBucket_givenBucketLock_createsReceiptAfterSuccessfulBucketCopy() {
		copyBucketEntryPoint.copyBucket(bucket);

		InOrder inOrder = inOrder(endpoint, receipts);
		inOrder.verify(endpoint).call(bucket);
		inOrder.verify(receipts).createReceipt(bucket);
	}

	public void copyBucket_endpointCallThrows_doesNotCreateReceipt() {
		doThrow(RuntimeException.class).when(endpoint).call(bucket);
		copyBucketEntryPoint.copyBucket(bucket);
		verify(receipts, never()).createReceipt(bucket);
	}

	public void copyBucket_notGivenBucketLock_doesNothingWithDependencies() {
		assertTrue(realBucketLocker.getLockForBucket(bucket).tryLockExclusive());
		copyBucketEntryPoint.copyBucket(bucket);
		verifyZeroInteractions(endpoint, receipts);
	}
}

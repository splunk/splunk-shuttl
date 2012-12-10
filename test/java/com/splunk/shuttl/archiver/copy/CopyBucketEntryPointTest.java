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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class CopyBucketEntryPointTest {

	private CopyBucketEntryPoint copyBucketEntryPoint;
	private CallCopyBucketEndpoint endpoint;
	private CopyBucketReceipts receipts;

	@BeforeMethod
	public void setUp() {
		endpoint = mock(CallCopyBucketEndpoint.class);
		receipts = mock(CopyBucketReceipts.class);
		copyBucketEntryPoint = new CopyBucketEntryPoint(endpoint, receipts);
	}

	public void copyBucket_givenBucket_createsReceiptAfterSuccessfulBucketCopy() {
		LocalBucket bucket = TUtilsBucket.createBucket();
		copyBucketEntryPoint.copyBucket(bucket);

		InOrder inOrder = inOrder(endpoint, receipts);
		inOrder.verify(endpoint).call(bucket);
		inOrder.verify(receipts).createReceipt(bucket);
	}

	public void copyBucket_endpointCallThrows_doesNotCreateReceipt() {
		LocalBucket bucket = mock(LocalBucket.class);
		doThrow(RuntimeException.class).when(endpoint).call(bucket);

		try {
			copyBucketEntryPoint.copyBucket(bucket);
			fail();
		} catch (RuntimeException e) {
		}
		verify(receipts, never()).createReceipt(bucket);
	}
}

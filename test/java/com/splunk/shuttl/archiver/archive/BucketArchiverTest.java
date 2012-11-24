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

import static org.mockito.Mockito.*;

import org.mockito.InOrder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;

@Test(groups = { "fast-unit" })
public class BucketArchiverTest {

	private BucketCopier bucketCopier;
	private BucketArchiver bucketArchiver;
	private BucketDeleter bucketDeleter;
	private LocalBucket bucket;

	@BeforeMethod
	public void setUp() {
		bucketCopier = mock(BucketCopier.class);
		bucketDeleter = mock(BucketDeleter.class);
		bucketArchiver = new BucketArchiver(bucketCopier, bucketDeleter);

		bucket = mock(LocalBucket.class);
	}

	public void archiveBucket_copierAndDeleter_copiesThenDeletes() {
		bucketArchiver.archiveBucket(bucket);

		InOrder inOrder = inOrder(bucketCopier, bucketDeleter);
		inOrder.verify(bucketCopier).copyBucket(bucket);
		inOrder.verify(bucketDeleter).deleteBucket(bucket);
	}

	public void archiveBucket_copierThrows_doesNotDelete() {
		doThrow(RuntimeException.class).when(bucketCopier).copyBucket(bucket);

		try {
			bucketArchiver.archiveBucket(bucket);
		} catch (RuntimeException e) {
		}
		verifyZeroInteractions(bucketDeleter);
	}
}

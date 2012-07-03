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
package com.splunk.shuttl.archiver.thaw;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketSizeResolverTest {

	private BucketSizeResolver bucketSizeResolver;
	private ArchiveBucketSize archiveBucketSize;
	private Bucket bucketWithoutSize;

	@BeforeMethod
	public void setUp() {
		archiveBucketSize = mock(ArchiveBucketSize.class);
		bucketSizeResolver = new BucketSizeResolver(archiveBucketSize);
		bucketWithoutSize = TUtilsBucket.createRemoteBucket();
	}

	public void testSetUp_givenBucketWithoutSize_bucketHasNoSize() {
		assertNull(bucketWithoutSize.getSize());
	}

	public void resolveBucketsSizes_givenBucketWithoutSize_createsBucketWithSize() {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		when(archiveBucketSize.getSize(remoteBucket)).thenReturn(4L);

		Bucket bucketWithSize = bucketSizeResolver.resolveBucketSize(remoteBucket);
		assertEquals(4, (long) bucketWithSize.getSize());
	}

	public void resolveBucketSizes_givenBucketWithoutSize_keepsAllOtherPropertiesThanSize() {
		Bucket sizedBucket = bucketSizeResolver
				.resolveBucketSize(bucketWithoutSize);
		assertEquals(bucketWithoutSize.getIndex(), sizedBucket.getIndex());
		assertEquals(bucketWithoutSize.getName(), sizedBucket.getName());
		assertEquals(bucketWithoutSize.getEarliest(), sizedBucket.getEarliest());
		assertEquals(bucketWithoutSize.getLatest(), sizedBucket.getLatest());
		assertEquals(bucketWithoutSize.getURI(), sizedBucket.getURI());
		assertFalse(bucketWithoutSize.getSize() == sizedBucket.getSize());
	}
}

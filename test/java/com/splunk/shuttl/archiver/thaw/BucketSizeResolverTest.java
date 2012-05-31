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

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.util.Collections;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketSizeResolverTest {

	private BucketSizeResolver bucketSizeResolver;
	private ArchiveBucketSize archiveBucketSize;

	@BeforeMethod
	public void setUp() {
		archiveBucketSize = mock(ArchiveBucketSize.class);
		bucketSizeResolver = new BucketSizeResolver(archiveBucketSize);
	}

	public void resolveBucketsSizes_givenEmptyList_emptyList() {
		List<Bucket> emptyList = Collections.emptyList();
		List<Bucket> resolveBucketsSizes = bucketSizeResolver
				.resolveBucketsSizes(emptyList);
		assertTrue(resolveBucketsSizes.isEmpty());
	}

	public void resolveBucketsSizes_givenBucketWithoutSize_createsBucketWithSize() {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		assertNull(remoteBucket.getSize());
		List<Bucket> bucketsWithoutSize = asList(remoteBucket);
		when(archiveBucketSize.getSize(remoteBucket)).thenReturn(4L);

		List<Bucket> bucketsWithSize = bucketSizeResolver
				.resolveBucketsSizes(bucketsWithoutSize);
		assertEquals(1, bucketsWithSize.size());
		Bucket bucketWithSize = bucketsWithSize.get(0);
		assertEquals(4, (long) bucketWithSize.getSize());
	}

	public void resolveBucketsSizes_givenTwoBucketsWithoutSize_twoBucketsWithSize() {
		Bucket remoteBucket = TUtilsBucket.createRemoteBucket();
		Bucket remoteBucket2 = TUtilsBucket.createRemoteBucket();
		assertNull(remoteBucket.getSize());
		assertNull(remoteBucket2.getSize());
		List<Bucket> bucketsWithoutSize = asList(remoteBucket, remoteBucket2);

		when(archiveBucketSize.getSize(any(Bucket.class))).thenReturn(47L);

		List<Bucket> bucketsWithSize = bucketSizeResolver
				.resolveBucketsSizes(bucketsWithoutSize);
		assertEquals(2, bucketsWithSize.size());
		for (Bucket bucket : bucketsWithSize)
			assertEquals(47, (long) bucket.getSize());
	}

	public void resolveBucketSizes_givenBucketWithoutSize_keepsAllOtherPropertiesThanSize() {
		Bucket bucket = TUtilsBucket.createRemoteBucket();
		List<Bucket> resolveBucketsSizes = bucketSizeResolver
				.resolveBucketsSizes(asList(bucket));
		Bucket sizedBucket = resolveBucketsSizes.get(0);
		assertEquals(bucket.getIndex(), sizedBucket.getIndex());
		assertEquals(bucket.getName(), sizedBucket.getName());
		assertEquals(bucket.getEarliest(), sizedBucket.getEarliest());
		assertEquals(bucket.getLatest(), sizedBucket.getLatest());
		assertEquals(bucket.getURI(), sizedBucket.getURI());
		assertFalse(bucket.getSize() == sizedBucket.getSize());
	}
}

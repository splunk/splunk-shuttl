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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.EntityCollection;
import com.splunk.Index;
import com.splunk.Service;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;

@Test(groups = { "fast-unit" })
public class ColdBucketInteratorTest {

	private Service splunkService;
	private BucketIteratorFactory bucketIteratorFactory;
	private ColdBucketInterator coldBucketInterator;
	private String index;

	@BeforeMethod
	public void setUp() {
		splunkService = mock(Service.class);
		bucketIteratorFactory = mock(BucketIteratorFactory.class);

		coldBucketInterator = new ColdBucketInterator(splunkService,
				bucketIteratorFactory);

		index = "index";
	}

	public void _givenIndex_returnBucketIteratorInColdDirectoryForIndex() {
		String coldPath = stubSplunkIndexColdPath(index);
		Iterable<LocalBucket> buckets = asList(mock(LocalBucket.class));

		when(
				bucketIteratorFactory.iteratorInDirectory(eq(new File(coldPath)),
						eq(index))).thenReturn(buckets);
		Iterable<LocalBucket> actualBuckets = coldBucketInterator
				.coldBucketsAtIndex(index);

		assertEquals(buckets, actualBuckets);
	}

	@SuppressWarnings("unchecked")
	private String stubSplunkIndexColdPath(String index) {
		EntityCollection<Index> indexes = mock(EntityCollection.class);
		when(splunkService.getIndexes()).thenReturn(indexes);
		Index splunkIndex = mock(Index.class);
		when(indexes.get(index)).thenReturn(splunkIndex);
		String coldPath = "/cold/path";
		when(splunkIndex.getColdPathExpanded()).thenReturn(coldPath);
		return coldPath;
	}

	@SuppressWarnings("unchecked")
	@Test(expectedExceptions = { IllegalIndexException.class })
	public void _givenServiceDoesNotHaveIndex_throws() {
		when(splunkService.getIndexes()).thenReturn(
				mock(EntityCollection.class, RETURNS_DEFAULTS));
		coldBucketInterator.coldBucketsAtIndex(index);
	}
}

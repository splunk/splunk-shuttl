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
package com.splunk.shuttl.archiver.listers;

import static java.util.Arrays.*;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.BucketFormatResolver;

@Test(groups = { "fast-unit" })
public class ListsBucketsFilteredTest {

	private ArchiveBucketsLister archiveBucketsLister;
	private BucketFilter bucketFilter;
	private BucketFormatResolver bucketFormatResolver;
	private ListsBucketsFiltered listsBucketsFiltered;
	private Date earliestTime;
	private Date latestTime;

	@BeforeMethod
	public void setUp() {
		earliestTime = mock(Date.class);
		latestTime = mock(Date.class);

		archiveBucketsLister = mock(ArchiveBucketsLister.class);
		bucketFilter = mock(BucketFilter.class);
		bucketFormatResolver = mock(BucketFormatResolver.class);
		listsBucketsFiltered = new ListsBucketsFiltered(archiveBucketsLister,
				bucketFilter, bucketFormatResolver);
	}

	@Test(groups = { "fast-unit" })
	public void _givenBucketsInAnIndex_filterBucketsOnTimeRange() {
		List<Bucket> bucketsInIndex = asList(mock(Bucket.class));
		String index = "index";
		when(archiveBucketsLister.listBucketsInIndex(index)).thenReturn(
				bucketsInIndex);
		listsBucketsFiltered.listFilteredBucketsAtIndex(index, earliestTime,
				latestTime);
		verify(bucketFilter).filterBucketsByTimeRange(bucketsInIndex, earliestTime,
				latestTime);
	}

	@SuppressWarnings("unchecked")
	public void _givenFilteredBuckets_resolvesFilteredBucketsFormats() {
		List<Bucket> filteredBuckets = asList(mock(Bucket.class));
		when(
				bucketFilter.filterBucketsByTimeRange(anyList(), any(Date.class),
						any(Date.class))).thenReturn(filteredBuckets);
		listsBucketsFiltered.listFilteredBucketsAtIndex("foo", earliestTime,
				latestTime);
		verify(bucketFormatResolver).resolveBucketsFormats(filteredBuckets);
	}

	@SuppressWarnings("unchecked")
	public void _givenBucketsWithFormats_returnThoseBuckets() {
		List<Bucket> bucketsWithFormats = asList(mock(Bucket.class));
		when(bucketFormatResolver.resolveBucketsFormats(anyList())).thenReturn(
				bucketsWithFormats);
		List<Bucket> filteredBucketsAtIndex = listsBucketsFiltered
				.listFilteredBucketsAtIndex("foo", earliestTime, latestTime);
		assertEquals(bucketsWithFormats, filteredBucketsAtIndex);
	}
}

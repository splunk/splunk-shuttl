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
import static org.testng.Assert.*;

import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsDate;

@Test(groups = { "fast-unit" })
public class BucketFilterTest {

	BucketFilter bucketFilter;
	Date earliest;
	Date latest;
	List<Bucket> filteredBuckets;

	@BeforeMethod(groups = { "fast-unit" })
	public void setUp() {
		long earliestTime = 1330000000;
		long latestTime = earliestTime + 1000;
		earliest = new Date(earliestTime);
		latest = new Date(latestTime);
		bucketFilter = new BucketFilter();
		filteredBuckets = null; // It's a field to remove List<Bucket>
		// boilerplate.
	}

	@Test(groups = { "fast-unit" })
	public void BucketFilterTest_setUp_earliestIsEarlierThanLatest() {
		assertTrue(earliest.before(latest));
	}

	@SuppressWarnings("unchecked")
	public void filterBucketsByTimeRange_earliestTimeIsLaterThanLatestTime_emptyList() {
		Date earlierThanEarliest = new Date(earliest.getTime() - 1000);
		assertTrue(earliest.after(earlierThanEarliest));
		filteredBuckets = bucketFilter.filterBucketsByTimeRange(mock(List.class),
				earliest, earlierThanEarliest);
		assertTrue(filteredBuckets.isEmpty());
	}

	private void filterBuckets(Bucket... buckets) {
		filteredBuckets = bucketFilter.filterBucketsByTimeRange(
				Arrays.asList(buckets), earliest, latest);
	}

	public void filterBucketsByTimeRange_noBuckets_emptyList() {
		filterBuckets(); // Nothing.
		assertTrue(filteredBuckets.isEmpty());
	}

	public void filterBucketsByTimeRange_givenBucketWithEarliestAndLatestEqualToFilterEarliest_doNotFilterBucket() {
		Bucket bucket = TUtilsBucket.createBucketWithTimes(earliest, earliest);
		filterBuckets(bucket);
		assertEquals(1, filteredBuckets.size());
		assertEquals(bucket, filteredBuckets.get(0));
	}

	public void filterBucketsByTimeRange_givenBucketWithEarliestAndLatestEqualToFilterLatest_doNotFilterBucket() {
		Bucket bucket = TUtilsBucket.createBucketWithTimes(latest, latest);
		filterBuckets(bucket);
		assertEquals(1, filteredBuckets.size());
		assertEquals(bucket, filteredBuckets.get(0));
	}

	public void filterBucketsByTimeRange_givenOneBucketWhereLatestIsBeforeFiltersEarliest_filterBucketThenReturnEmptyList() {
		Date bucketLatest = new Date(earliest.getTime() - 100);
		assertTrue(bucketLatest.before(earliest));

		Bucket bucket = createBucketWithEarliestAndLatestSetToDate(bucketLatest);
		filterBuckets(bucket);
		assertTrue(filteredBuckets.isEmpty());
	}

	public void filterBucketsByTimeRange_givenOneBucketWhereEarliestIsAfterFiltersLatest_filterBucketAndReturnEmptyList() {
		Date bucketEarliest = new Date(latest.getTime() + 1000);
		assertTrue(bucketEarliest.after(latest));

		Bucket bucket = createBucketWithEarliestAndLatestSetToDate(bucketEarliest);
		filterBuckets(bucket);
		assertTrue(filteredBuckets.isEmpty());
	}

	public void filterBucketsByTimeRange_givenTwoBucketsWithEarliestAndLatestWithinFilterTimeRange_returnBothBuckets() {
		Bucket bucket1 = TUtilsBucket.createBucketWithTimes(earliest, latest);
		Bucket bucket2 = TUtilsBucket.createBucketWithTimes(earliest, latest);
		filterBuckets(bucket1, bucket2);
		assertEquals(2, filteredBuckets.size());
		assertTrue(filteredBuckets.containsAll(Arrays.asList(bucket1, bucket2)));
	}

	private Bucket createBucketWithEarliestAndLatestSetToDate(Date date) {
		return TUtilsBucket.createBucketWithTimes(date, date);
	}

	public void filterBucketsByTimeRange_givenEarliestEqualsLatest_returnTheBucketAtThatTimeRange() {
		Date earliestAndLatest = TUtilsDate.getNowWithoutMillis();
		Bucket bucket = TUtilsBucket.createBucketWithTimes(earliestAndLatest,
				earliestAndLatest);
		List<Bucket> filteredBuckets = bucketFilter.filterBucketsByTimeRange(
				Arrays.asList(bucket), earliestAndLatest, earliestAndLatest);
		filterBuckets(bucket);
		assertEquals(1, filteredBuckets.size());
		assertTrue(filteredBuckets.contains(bucket));
	}

}

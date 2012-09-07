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
package com.splunk.shuttl.archiver.model;

import static org.testng.AssertJUnit.*;

import java.io.FileNotFoundException;
import java.util.Calendar;
import java.util.Date;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsDate;

@Test(groups = { "fast-unit" })
public class BucketTimesTest {

	Date earliest;
	Date latest;

	@BeforeMethod
	public void setUp() {
		earliest = TUtilsDate.getNowWithoutMillis();
		latest = new Date(earliest.getTime() + 1000);
	}

	@Test(groups = { "fast-unit" })
	public void setUp_givenEarliestAndLatestDates_earliestIsBeforeLatest() {
		assertTrue(earliest.before(latest));
	}

	@Test(groups = { "fast-unit" })
	public void getEarliest_givenEarliest_earliest() {
		Bucket bucket = TUtilsBucket.createBucketWithTimes(earliest, new Date());
		assertEquals(earliest, bucket.getEarliest());
	}

	public void getLatest_givenLatest_latest() {
		Bucket bucket = TUtilsBucket.createBucketWithTimes(new Date(), latest);
		assertEquals(latest, bucket.getLatest());
	}

	public void gettingTimes_bucketNameWithTimeFrom2012InSeconds_DateObjectInMilliseconds()
			throws FileNotFoundException, FileNotDirectoryException {
		long time = 1346822652; // Sep 4, 2012 (PST)
		Bucket bucket = new Bucket(null, null, "db_" + time + "_" + time + "_0",
				null);

		assertEquals(2012, getYear(bucket.getEarliest()));
		assertEquals(2012, getYear(bucket.getLatest()));
	}

	private int getYear(Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		return cal.get(Calendar.YEAR);
	}

}

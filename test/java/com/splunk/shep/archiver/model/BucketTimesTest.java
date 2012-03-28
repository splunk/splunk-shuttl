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
package com.splunk.shep.archiver.model;

import static org.testng.AssertJUnit.*;

import java.util.Date;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast-unit" })
public class BucketTimesTest {

    Date earliest;
    Date latest;

    @BeforeMethod
    public void setUp() {
	earliest = new Date(12345678);
	latest = new Date(earliest.getTime() + 100);
    }

    @Test(groups = { "fast-unit" })
    public void setUp_givenEarliestAndLatestDates_earliestIsBeforeLatest() {
	assertTrue(earliest.before(latest));
    }

    @Test(groups = { "fast-unit" })
    public void getEarliest_givenEarliest_earliest() {
	Bucket bucket = UtilsBucket.createBucketWithTimes(earliest, new Date());
	assertEquals(earliest, bucket.getEarliest());
    }

    public void getLatest_givenLatest_latest() {
	Bucket bucket = UtilsBucket.createBucketWithTimes(new Date(), latest);
	assertEquals(latest, bucket.getLatest());
    }

}

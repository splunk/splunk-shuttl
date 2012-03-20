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
package com.splunk.shep.archiver.thaw;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.util.Date;
import java.util.List;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;

@Test(groups = { "fast" })
public class BucketFilterTest {

    BucketFilter bucketFilter;
    Date earliest;
    Date latest;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	long earliestTime = 1330000000;
	long latestTime = earliestTime + 1000;
	earliest = new Date(earliestTime);
	latest = new Date(latestTime);
	bucketFilter = new BucketFilter();
    }

    public void BucketFilterTest_setUp_earliestIsEarlierThanLatest() {
	assertTrue(earliest.before(latest));
    }

    @SuppressWarnings("unchecked")
    public void filterBucketsByTimeRange_earliestTimeIsLaterThanLatestTime_emptyList() {
	Date earlierThanEarliest = new Date(earliest.getTime() - 1000);
	assertTrue(earliest.after(earlierThanEarliest));
	List<Bucket> filteredBuckets = bucketFilter.filterBucketsByTimeRange(
		mock(List.class), earliest, earlierThanEarliest);
	assertTrue(filteredBuckets.isEmpty());
    }

}

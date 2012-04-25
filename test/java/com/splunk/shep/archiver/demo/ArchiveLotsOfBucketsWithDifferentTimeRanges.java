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
package com.splunk.shep.archiver.demo;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsMBean;

/**
 * Test for generating a lot of demo data, that can then be showed off manually.
 */
@Test(enabled = false, groups = { "functional" })
public class ArchiveLotsOfBucketsWithDifferentTimeRanges {

    long MILLI_SECONDS_IN_A_DAY = 86400000;
    int TWO_YEARS_OF_DAYS = 365 * 2;
    private BucketArchiver bucketArchiver;

    @BeforeMethod
    public void setUp() {
	UtilsMBean.registerShepArchiverMBean();
	bucketArchiver = BucketArchiverFactory.createConfiguredArchiver();
    }

    public void _archiveLotsOfBucketsWithDifferentTimeRanges_() {
	archiveOneBucketPerDay();
    }

    private void archiveOneBucketPerDay() {
	Date startDate = new Date(); // Today.
	Date earliest = startDate;
	for (int i = 0; i < TWO_YEARS_OF_DAYS; i++) {
	    Date latest = new Date(earliest.getTime());
	    Bucket bucket = UtilsBucket.createBucketWithIndexAndTimeRange(
		    "shep", earliest, latest);
	    bucketArchiver.archiveBucket(bucket);
	    earliest = new Date(latest.getTime() + 1 + MILLI_SECONDS_IN_A_DAY);
	}
	Date lastDate = earliest;
	printInfoAboutData(startDate, lastDate);
    }

    private void printInfoAboutData(Date startDate, Date lastDate) {
	Date diffDate = new Date(lastDate.getTime() - startDate.getTime());

	System.out.println("Start date: " + startDate + ", as time: "
		+ startDate.getTime());
	System.out.println("Last date: " + lastDate + ", as time: "
		+ lastDate.getTime());
	System.out.println("Time between dates: " + diffDate.getTime()
		+ ", as date: " + new SimpleDateFormat().format(diffDate));
    }
}

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
package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;
import com.splunk.shep.testutil.UtilsFile;

@Test(groups = { "fast" })
public class FailedBucketsHandlerTest {

    FailedBucketTransfers failedBucketTransfers;
    File failedBucketLocation;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	failedBucketLocation = UtilsFile.createTempDirectory();
	failedBucketTransfers = new FailedBucketTransfers(
		failedBucketLocation.getAbsolutePath());
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(failedBucketLocation);
    }

    public void getFailedBuckets_failedLocationDoesNotExist_emptyList() {
	assertTrue(failedBucketLocation.delete());
	assertTrue(!failedBucketLocation.exists());
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertTrue(failedBuckets.isEmpty());
    }

    public void getFailedBuckets_givenBucketInFailedLocation_returnsListContainingTheFailedBucket()
	    throws FileNotFoundException, IOException {
	File directoryRepresentingIndex = UtilsFile.createDirectoryInParent(
		failedBucketLocation, "index");
	Bucket failedBucket = UtilsBucket
		.createBucketInDirectory(directoryRepresentingIndex);
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertEquals(1, failedBuckets.size());
	assertEquals(failedBucket.getDirectory().getAbsolutePath(),
		failedBuckets.get(0).getDirectory().getAbsolutePath());
    }
}

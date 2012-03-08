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
package com.splunk.shep.archiver.archive.recovery;

import static com.splunk.shep.testutil.UtilsFile.*;
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
public class FailedBucketTransfersTest {

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
	Bucket failedBucket = createFailedBucket("index");
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertEquals(1, failedBuckets.size());
	assertEquals(failedBucket, failedBuckets.get(0));
    }

    public void getFailedBuckets_givenNoBucketsInFailedLocation_emptyList() {
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertTrue(failedBuckets.isEmpty());
    }

    public void getFailedBuckets_givenTwoBucketsInFailedLocation_listWithTheTwoBuckets() {
	Bucket failedBucket1 = createFailedBucket("a");
	Bucket failedBucket2 = createFailedBucket("b");
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertEquals(2, failedBuckets.size());
	assertTrue(failedBuckets.contains(failedBucket1));
	assertTrue(failedBuckets.contains(failedBucket2));
    }

    // This test knows too much about the inner structure of the moved buckets.
    // The test should probably be removed if/when it fails.
    public void moveFailedBucket_givenBucket_movedBucketTo_FailedLocation_Index_BucketName() {
	assertTrue(isDirectoryEmpty(failedBucketLocation));
	Bucket failedBucket = UtilsBucket.createTestBucket();
	File movedBucket = new File(failedBucketLocation.getAbsolutePath()
		+ File.separator + failedBucket.getIndex() + File.separator
		+ failedBucket.getName());
	assertTrue(!movedBucket.exists());

	failedBucketTransfers.moveFailedBucket(failedBucket);
	assertTrue(!isDirectoryEmpty(failedBucketLocation));
	assertTrue(movedBucket.exists());
    }

    public void getFailedBuckets_afterSuccessfullyMovedABucketUsingMoveFailedBucket_getBucketThatFailedAndMoved() {
	Bucket failedBucket = UtilsBucket.createTestBucket();
	failedBucketTransfers.moveFailedBucket(failedBucket);

	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertEquals(1, failedBuckets.size());
	Bucket actualBucket = failedBuckets.get(0);
	assertEquals(failedBucket.getIndex(), actualBucket.getIndex());
	assertEquals(failedBucket.getName(), actualBucket.getName());
	assertEquals(failedBucket.getFormat(), actualBucket.getFormat());
    }

    public void getFailedBuckets_afterCreatingLockInFailedLocation_emptyList() {
	File lock = UtilsFile.createFileInParent(failedBucketLocation, "lock");
	assertTrue(lock.isFile());
	List<Bucket> failedBuckets = failedBucketTransfers.getFailedBuckets();
	assertTrue(failedBuckets.isEmpty());
    }

    private Bucket createFailedBucket(String index) {
	File directoryRepresentingIndex = UtilsFile.createDirectoryInParent(
		failedBucketLocation, index);
	return UtilsBucket.createBucketInDirectoryWithIndex(
		directoryRepresentingIndex, index);
    }
}

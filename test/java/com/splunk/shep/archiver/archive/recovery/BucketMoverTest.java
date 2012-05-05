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

@Test(groups = { "fast-unit" })
public class BucketMoverTest {

    BucketMover bucketMover;
    File moveBucketLocation;

    @BeforeMethod(groups = { "fast-unit" })
    public void setUp() {
	moveBucketLocation = UtilsFile.createTempDirectory();
	bucketMover = new BucketMover(moveBucketLocation);
    }

    @AfterMethod(groups = { "fast-unit" })
    public void tearDown() throws IOException {
	FileUtils.deleteDirectory(moveBucketLocation);
    }

    @Test(groups = { "fast-unit" })
    public void getMovedBuckets_moveLocationDoesNotExist_emptyList() {
	assertTrue(moveBucketLocation.delete());
	assertTrue(!moveBucketLocation.exists());
	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertTrue(movedBuckets.isEmpty());
    }

    public void getMovedBuckets_givenBucketInMoveLocation_returnsListContainingTheMovedBucket()
	    throws FileNotFoundException, IOException {
	Bucket bucket = createBucketInMoveLocationWithIndexPreserved("index");
	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertEquals(1, movedBuckets.size());
	assertEquals(bucket, movedBuckets.get(0));
    }

    public void getMovedBuckets_givenNoBucketsInMoveLocation_emptyList() {
	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertTrue(movedBuckets.isEmpty());
    }

    public void getMovedBuckets_givenTwoBucketsWithDifferentIndexInMoveLocation_listWithTheTwoBuckets() {
	Bucket movedBucketIndex = createBucketInMoveLocationWithIndexPreserved("a");
	Bucket movedBucketAnotherIndex = createBucketInMoveLocationWithIndexPreserved("b");
	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertEquals(2, movedBuckets.size());
	assertTrue(movedBuckets.contains(movedBucketIndex));
	assertTrue(movedBuckets.contains(movedBucketAnotherIndex));
    }

    public void getMovedBuckets_givenTwoBucketsWithSameIndexInMoveLocation_listWithTheTwoBuckets() {
	String index = "a";
	Bucket movedBucket = createBucketInMoveLocationWithIndexPreserved(index);
	Bucket movedBucketSameIndex = UtilsBucket
		.createBucketInDirectoryWithIndex(movedBucket.getDirectory()
			.getParentFile(), index);

	// Assertions on buckets.
	assertEquals(movedBucket.getIndex(), movedBucketSameIndex.getIndex());
	assertEquals(movedBucket.getDirectory().getParent(),
		movedBucketSameIndex.getDirectory().getParent());

	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertEquals(2, movedBuckets.size());
	assertTrue(movedBuckets.contains(movedBucket));
	assertTrue(movedBuckets.contains(movedBucketSameIndex));
    }

    public void moveBucket_givenBucket_movedBucketTo_moveLocation_Index_BucketName() {
	assertTrue(isDirectoryEmpty(moveBucketLocation));
	Bucket bucketToMove = UtilsBucket.createTestBucket();

	Bucket movedBucket = bucketMover.moveBucket(bucketToMove);
	assertTrue(!isDirectoryEmpty(moveBucketLocation));
	assertTrue(movedBucket.getDirectory().exists());
    }

    public void getMovedBuckets_afterSuccessfullyMovedABucketUsingMoveBucketToMove_getBucketThatMoved() {
	Bucket bucketToMove = UtilsBucket.createTestBucket();
	bucketMover.moveBucket(bucketToMove);

	List<Bucket> movedBucket = bucketMover.getMovedBuckets();
	assertEquals(1, movedBucket.size());
	Bucket actualBucket = movedBucket.get(0);
	assertEquals(bucketToMove.getIndex(), actualBucket.getIndex());
	assertEquals(bucketToMove.getName(), actualBucket.getName());
	assertEquals(bucketToMove.getFormat(), actualBucket.getFormat());
    }

    public void getMovedBuckets_afterCreatingLockInMoveLocation_emptyList() {
	File lock = UtilsFile.createFileInParent(moveBucketLocation, "lock");
	assertTrue(lock.isFile());
	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertTrue(movedBuckets.isEmpty());
    }

    public void getMovedBuckets_noBucketsInIndexDirectory_emptyList() {
	File empty = createDirectoryInParent(moveBucketLocation, "index");
	assertTrue(isDirectoryEmpty(empty));

	List<Bucket> movedBuckets = bucketMover.getMovedBuckets();
	assertTrue(movedBuckets.isEmpty());
    }

    private Bucket createBucketInMoveLocationWithIndexPreserved(String index) {
	File directoryRepresentingIndex = UtilsFile.createDirectoryInParent(
		moveBucketLocation, index);
	return UtilsBucket.createBucketInDirectoryWithIndex(
		directoryRepresentingIndex, index);
    }
}

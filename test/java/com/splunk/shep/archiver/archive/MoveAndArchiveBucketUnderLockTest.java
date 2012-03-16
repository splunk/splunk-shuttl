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

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.testutil.UtilsBucket;

@Test(groups = { "fast" })
public class MoveAndArchiveBucketUnderLockTest {

    MoveAndArchiveBucketUnderLock moveAndArchiveBucketUnderLock;
    BucketMover bucketMover;
    Bucket bucket;
    ArchiveRestHandler archiveRestHandler;

    @BeforeMethod
    public void setUp() {
	bucket = UtilsBucket.createTestBucket();
	bucketMover = mock(BucketMover.class);
	archiveRestHandler = mock(ArchiveRestHandler.class);
	moveAndArchiveBucketUnderLock = new MoveAndArchiveBucketUnderLock(
		bucketMover, archiveRestHandler);
    }

    public void moveThenArchiveBucket_givenBucket_movesTheBucket() {
	moveAndArchiveBucketUnderLock.moveThenArchiveBucket(bucket);
	verify(bucketMover).moveBucket(bucket);
    }

    public void moveThenArchiveBucket_givenBucket_archivesTheMovedBucket() {
	Bucket movedBucket = mock(Bucket.class);
	when(bucketMover.moveBucket(bucket)).thenReturn(movedBucket);
	moveAndArchiveBucketUnderLock.moveThenArchiveBucket(bucket);
	verify(archiveRestHandler).callRestToArchiveBucket(movedBucket);
    }

    public void handleLockedBucket_givenBucket_movesAndArchivesTheBucket() {
	moveAndArchiveBucketUnderLock.handleLockedBucket(bucket);
	verify(bucketMover).moveBucket(bucket);
	verify(archiveRestHandler).callRestToArchiveBucket(any(Bucket.class));
	verifyNoMoreInteractions(bucketMover, archiveRestHandler);
    }

}

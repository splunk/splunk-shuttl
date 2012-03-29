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

import com.splunk.shep.archiver.archive.recovery.BucketLocker.SharedLockBucketHandler;
import com.splunk.shep.archiver.archive.recovery.BucketMover;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Moves the bucket with the {@link BucketMover} and archives it with the
 * {@link ArchiveRestHandler}. <br/>
 * <br/>
 * Implements {@link SharedLockBucketHandler} so it can do this safely with the
 * {@link Bucket} locked.
 */
public class MoveAndArchiveBucketUnderLock implements SharedLockBucketHandler {

    private final BucketMover bucketMover;
    private final ArchiveRestHandler archiveRestHandler;

    /**
     * @param bucketMover
     * @param archiveRestHandler
     */
    public MoveAndArchiveBucketUnderLock(BucketMover bucketMover,
	    ArchiveRestHandler archiveRestHandler) {
	this.bucketMover = bucketMover;
	this.archiveRestHandler = archiveRestHandler;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.archiver.archive.recovery.BucketLocker.LockedBucketHandler
     * #handleLockedBucket(com.splunk.shep.archiver.model.Bucket)
     */
    @Override
    public void handleSharedLockedBucket(Bucket bucket) {
	moveThenArchiveBucket(bucket);
    }

    /**
     * @param bucket
     *            to move and archive
     */
    public void moveThenArchiveBucket(Bucket bucket) {
	Bucket movedBucket = bucketMover.moveBucket(bucket);
	archiveRestHandler.callRestToArchiveBucket(movedBucket);
    }

}

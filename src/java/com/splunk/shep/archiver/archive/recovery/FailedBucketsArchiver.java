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

import static com.splunk.shep.archiver.LogFormatter.*;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.archive.recovery.BucketLocker.SharedLockBucketHandler;
import com.splunk.shep.archiver.model.Bucket;

/**
 * Lets a {@link FailedBucketRecoveryHandler} recover buckets that failed to
 * archive. Make sure that the recovering of buckets is only done by 1 Java
 * Virtual Machines, at one time, by synchronizing with {@link BucketLocker}.
 */
public class FailedBucketsArchiver {

    private final BucketMover bucketMover;
    private final BucketLocker bucketLocker;
    private static Logger logger = Logger
	    .getLogger(FailedBucketsArchiver.class);

    /**
     * @param bucketMover
     *            for getting the failed buckets
     */
    public FailedBucketsArchiver(BucketMover bucketMover,
	    BucketLocker bucketLocker) {
	this.bucketMover = bucketMover;
	this.bucketLocker = bucketLocker;
    }

    /**
     * Recover failed buckets with handler. Any bucket that is available for
     * recovery is sent to the {@link SharedLockBucketHandler}.
     * 
     * @param bucketHandler
     *            to handle the moved buckets. Executes the handler if it was
     *            possible to get a lock on the bucket.
     */
    public void archiveFailedBuckets(SharedLockBucketHandler bucketHandler) {
	logger.debug(will("Archiving failed buckets", "failed buckets",
		bucketMover.getMovedBuckets()));
	
	for (Bucket movedBucket : bucketMover.getMovedBuckets()) {
	    bucketLocker.callBucketHandlerUnderSharedLock(movedBucket,
		    bucketHandler);
	}
    }

}

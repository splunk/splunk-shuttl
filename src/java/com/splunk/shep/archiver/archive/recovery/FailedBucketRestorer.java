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

import com.splunk.shep.archiver.model.Bucket;

/**
 * Lets a {@link FailedBucketRecoveryHandler} recover buckets that failed to
 * archive. Make sure that the recovering of buckets is only done by 1 Java
 * Virtual Machines, at one time, by synchronizing with {@link FailedBucketLock}
 * . The failed buckets recovered, are the buckets moved with
 * {@link BucketMover#moveBucket(Bucket)}.
 */
public class FailedBucketRestorer {

    private final BucketMover bucketMover;
    private final FailedBucketLock failedBucketLock;

    /**
     * @param bucketMover
     *            for getting the failed buckets
     * @param failedBucketLock
     *            for making sure that no one else is accessing the failed
     *            buckets.
     */
    public FailedBucketRestorer(BucketMover bucketMover,
	    FailedBucketLock failedBucketLock) {
	this.bucketMover = bucketMover;
	this.failedBucketLock = failedBucketLock;
    }

    /**
     * Recover failed buckets with handler. Any bucket that is available for
     * recovery is sent to the {@link FailedBucketRecoveryHandler}.
     * 
     * @param bucketHandler
     */
    public void recoverFailedBuckets(FailedBucketRecoveryHandler bucketHandler) {
	if (failedBucketLock.tryLock()) {
	    recoverEachFailedBucket(bucketHandler);
	}
	failedBucketLock.closeLock();
    }

    private void recoverEachFailedBucket(
	    FailedBucketRecoveryHandler bucketHandler) {
	for (Bucket failedBucket : bucketMover.getMovedBuckets()) {
	    bucketHandler.recoverFailedBucket(failedBucket);
	}
    }
}

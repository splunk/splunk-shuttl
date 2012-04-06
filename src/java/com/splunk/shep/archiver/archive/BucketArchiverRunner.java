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

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;

import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.archive.recovery.SimpleFileLock.NotLockedException;
import com.splunk.shep.archiver.model.Bucket;

/**
 * The {@link BucketArchiverRunner} makes sure that the bucket is locked,
 * archived, deleted after successful archiving, unlocked and deletion of lock
 * file.
 */
public class BucketArchiverRunner implements Runnable {

    private final BucketArchiver bucketArchiver;
    private final Bucket bucket;
    private final BucketLock bucketLock;

    /**
     * @param bucketArchiver
     *            for archiving a bucket.
     * @param bucket
     *            to archive.
     * @param bucketLock
     *            which is already locked.
     */
    public BucketArchiverRunner(BucketArchiver bucketArchiver, Bucket bucket,
	    BucketLock bucketLock) {
	if (!bucketLock.isLocked()) {
	    throw new NotLockedException("Bucket Lock has to be locked already"
		    + " before archiving bucket");
	}
	this.bucketArchiver = bucketArchiver;
	this.bucket = bucket;
	this.bucketLock = bucketLock;
    }

    @Override
    public void run() {
	if (!bucketLock.isLocked()) {
	    did("Was going to archive bucket",
		    "Bucket was not locked before archiving it",
		    "Bucket must have been locked before archiving, "
			    + "to make sure that it is safe to transfer "
			    + "the bucket to archive without "
			    + "any other modification.", "bucket", bucket);
	    throw new IllegalStateException("Bucket should still be locked"
		    + " when starting to archive "
		    + "the bucket. Aborting archiving.");
	} else {
	    archiveBucket();
	}
    }

    private void archiveBucket() {
	try {
	    bucketArchiver.archiveBucket(bucket);
	    deleteBucketWithErrorHandling(bucket);
	} finally {
	    bucketLock.deleteLockFile();
	    bucketLock.closeLock();
	}
    }

    private void deleteBucketWithErrorHandling(Bucket bucket) {
	try {
	    bucket.deleteBucket();
	} catch (IOException e) {
	    warn("Deleted a bucket from local file system, "
		    + "because archiving was complete.", e,
		    "Will ignore this exception", "bucket", bucket,
		    "exception", e);
	}
    }

}

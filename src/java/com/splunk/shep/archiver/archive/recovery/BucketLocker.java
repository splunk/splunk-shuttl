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
 * Class for locking buckets, synchronizing usages of buckets by locking a
 * bucket before using/modifying it.
 */
public class BucketLocker {

    /**
     * Executes {@link Runnable} while the bucket it locked. <br/>
     * Runnable is not executed if the bucket cannot be locked.
     */
    public void runWithBucketLocked(Bucket bucket, Runnable runnable) {
	executeRunnableDuringBucketLock(new BucketLock(bucket), runnable);

    }

    /**
     * Method exists for verifying that {@link BucketLock} is closed, whether it
     * gets the lock or not.
     */
    /* package-private */void executeRunnableDuringBucketLock(
	    BucketLock bucketLock, Runnable runnable) {
	if (bucketLock.tryLock())
	    runnable.run();
	bucketLock.closeLock();
    }

}

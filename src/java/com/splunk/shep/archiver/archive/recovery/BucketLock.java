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

import java.io.File;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.util.UtilsFile;

/**
 * Lock for a {@link Bucket} to make sure that no other JVM is messing with the
 * same bucket.
 */
public class BucketLock extends SimpleFileLock {

    private final Bucket bucket;

    /**
     * @param bucket
     *            to create lock for.
     */
    public BucketLock(Bucket bucket) {
	super(UtilsFile.getFileOutputStreamSilent(getLockFile(bucket))
		.getChannel());
	this.bucket = bucket;
    }

    /**
     * @return File that controls the lock of this bucket.
     */
    /* package-private */File getLockFile() {
	return BucketLock.getLockFile(bucket);
    }

    private static File getLockFile(Bucket bucket) {
	File parentFileToBucket = bucket.getDirectory().getParentFile();
	File lock = new File(parentFileToBucket, "bucket-" + bucket.getName()
		+ ".lock");
	UtilsFile.touch(lock);
	return lock;
    }
}

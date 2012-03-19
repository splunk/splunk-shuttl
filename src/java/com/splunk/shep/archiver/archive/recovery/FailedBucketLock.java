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
import java.io.FileOutputStream;
import java.nio.channels.FileChannel;

import com.splunk.shep.archiver.archive.BucketFreezer;
import com.splunk.shep.archiver.util.UtilsFile;

/**
 * Lock for the failed buckets.
 */
public class FailedBucketLock extends SimpleFileLock {

    // Package private because of testing. Should not test with the real file,
    // since it might conflict with the environment the tests are run on.
    /* package-private */static String FAIL_BUCKET_LOCK_FILE_NAME = "buckets.lock";

    public FailedBucketLock() {
	super(getFailedBucketLockFileChannel());
    }

    /**
     * @return {@link FileChannel} to the file that acts as a lock for the
     *         failed buckets.
     */
    private static FileChannel getFailedBucketLockFileChannel() {
	File failedBucketsLockFile = getLockFile();
	UtilsFile.touch(failedBucketsLockFile);
	FileOutputStream outputStream = UtilsFile
		.getFileOutputStreamSilent(failedBucketsLockFile);
	return outputStream.getChannel();
    }

    /* package-private */static File getLockFile() {
	File failedBucketsLockFile = new File(
		BucketFreezer.DEFAULT_FAIL_LOCATION, FAIL_BUCKET_LOCK_FILE_NAME);
	return failedBucketsLockFile;
    }
}

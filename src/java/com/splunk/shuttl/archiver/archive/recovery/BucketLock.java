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
package com.splunk.shuttl.archiver.archive.recovery;

import static com.splunk.shuttl.archiver.LocalFileSystemConstants.*;

import java.io.File;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.util.UtilsFile;

/**
 * Lock for a {@link Bucket} to make sure that no other JVM is messing with the
 * same bucket.
 */
public class BucketLock extends SimpleFileLock {

	private final File lockFile;

	/**
	 * @param bucket
	 *          to create lock for.
	 */
	public BucketLock(Bucket bucket) {
		this(bucket, getLocksDirectory());
	}

	public BucketLock(Bucket bucket, File locksDirectory) {
		super(UtilsFile.getRandomAccessFileSilent(
				(getLockFile(bucket, locksDirectory))).getChannel());
		this.lockFile = getLockFile(bucket, locksDirectory);
	}

	/**
	 * @return File that controls the lock of this bucket.
	 */
	/* package-private */File getLockFile() {
		return lockFile;
	}

	private static File getLockFile(Bucket bucket, File locksDirectory) {
		File lock = new File(locksDirectory, "bucket-" + bucket.getName() + ".lock");
		UtilsFile.touch(lock);
		return lock;
	}

	/**
	 * Deletes the lock file only if it there's no one that has the file locked.
	 * 
	 * @return true if the file was deleted, false otherwise.
	 */
	public boolean deleteLockFile() {
		return getLockFile().delete();
	}
}

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
package com.splunk.shuttl.archiver.bucketlock;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.List;

import org.apache.log4j.Logger;

public class BucketLockCleaner {

	private static final Logger logger = Logger
			.getLogger(BucketLockCleaner.class);

	public static void closeLocks(List<BucketLock> bucketLocks) {
		for (BucketLock lock : bucketLocks)
			cleanLock(lock);
	}

	private static void cleanLock(BucketLock lock) {
		try {
			lock.closeLock();
		} catch (Exception e) {
			logger.warn(warn("Tried closing bucket lock", e, "will do nothing",
					"lock_file", lock.getLockFile()));
		}
	}

	public static void deleteLocks(List<BucketLock> bucketLocks) {
		for (BucketLock lock : bucketLocks)
			deleteLock(lock);
	}

	private static void deleteLock(BucketLock lock) {
		try {
			lock.deleteLockFile();
		} catch (Exception e) {
			logger.warn(warn("Tried deleting lock file", e, "will do nothing",
					"lock_file", lock.getLockFile()));
		}
	}
}

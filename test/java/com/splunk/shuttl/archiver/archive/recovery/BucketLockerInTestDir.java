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

import java.io.File;

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * @author periksson
 * 
 */
public class BucketLockerInTestDir extends BucketLocker {

	private final File directory;

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.archiver.archive.recovery.BucketLocker#getLockForBucket
	 * (com.splunk.shuttl.archiver.model.Bucket)
	 */
	/**
	 * @param directory
	 *          to store temporary locks in.
	 */
	public BucketLockerInTestDir(File directory) {
		this.directory = directory;
	}

	@Override
	protected BucketLock getLockForBucket(Bucket bucket) {
		return new BucketLock(bucket, directory);
	}
}

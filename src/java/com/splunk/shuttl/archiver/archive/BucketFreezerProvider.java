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
package com.splunk.shuttl.archiver.archive;

import com.splunk.shuttl.archiver.ArchiverMBeanNotRegisteredException;
import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLocker;
import com.splunk.shuttl.archiver.archive.recovery.FailedBucketsArchiver;
import com.splunk.shuttl.archiver.archive.recovery.IndexPreservingBucketMover;
import com.splunk.shuttl.archiver.bucketlock.BucketLocker;

/**
 * @author periksson
 * 
 */
public class BucketFreezerProvider {

	/**
	 * The construction logic for creating a {@link BucketFreezer}.<br/>
	 * <br/>
	 * Needs to have Archiver MBean registered before creating BucketFreezer, due
	 * to LocalFileSystemPaths.create().
	 * 
	 * @throws ArchiverMBeanNotRegisteredException
	 *           if archiver MBean is not registered.
	 */
	public BucketFreezer getConfiguredBucketFreezer() {
		IndexPreservingBucketMover bucketMover = IndexPreservingBucketMover
				.create(LocalFileSystemPaths.create().getSafeDirectory());
		BucketLocker bucketLocker = new ArchiveBucketLocker();
		FailedBucketsArchiver failedBucketsArchiver = new FailedBucketsArchiver(
				bucketMover, bucketLocker);
		ArchiveRestHandler archiveRestHandler = ArchiveRestHandler.create();

		return new BucketFreezer(bucketMover, bucketLocker, archiveRestHandler,
				failedBucketsArchiver);
	}

}

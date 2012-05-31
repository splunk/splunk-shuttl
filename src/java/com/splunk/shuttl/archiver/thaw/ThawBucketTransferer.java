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
package com.splunk.shuttl.archiver.thaw;

import java.io.File;
import java.io.IOException;

import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.BucketFactory;

/**
 * Transfers bucket to thaw.
 */
public class ThawBucketTransferer {

	private final ThawLocationProvider thawLocationProvider;
	private final ArchiveFileSystem archiveFileSystem;
	private final BucketFactory bucketFactory;

	public ThawBucketTransferer(ThawLocationProvider thawLocationProvider,
			ArchiveFileSystem archiveFileSystem, BucketFactory bucketFactory) {
		this.thawLocationProvider = thawLocationProvider;
		this.archiveFileSystem = archiveFileSystem;
		this.bucketFactory = bucketFactory;
	}

	/**
	 * Transfers an archived bucket in the thaw directory of the bucket's index.
	 * 
	 * @return the transferred bucket.
	 */
	public Bucket transferBucketToThaw(Bucket bucket) throws IOException {
		File bucketsThawLocation = thawLocationProvider
				.getLocationInThawForBucket(bucket);
		archiveFileSystem.getFile(bucketsThawLocation, bucket.getURI());
		return bucketFactory.createWithIndexDirectoryAndSize(bucket.getIndex(),
				bucketsThawLocation, bucket.getSize());
	}
}
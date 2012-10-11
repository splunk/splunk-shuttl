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

import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Resolves sizes for buckets that has been thawed.
 */
public class BucketSizeResolver {

	private final ArchiveBucketSize archiveBucketSize;

	/**
	 * @param archiveBucketSize
	 *          to get the size from the archive.
	 */
	public BucketSizeResolver(ArchiveBucketSize archiveBucketSize) {
		this.archiveBucketSize = archiveBucketSize;
	}

	/**
	 * @param bucket
	 *          that needs size to be resolved from the archive.
	 */
	public Bucket resolveBucketSize(Bucket bucket) {
		return createBucketWithSize(bucket);
	}

	private Bucket createBucketWithSize(Bucket bucket) {
		long size = archiveBucketSize.getSize(bucket);
		return createBucketWithErrorHandling(bucket, size);
	}

	private Bucket createBucketWithErrorHandling(Bucket bucket, long size) {
		return new Bucket(bucket.getPath(), bucket.getIndex(), bucket.getName(),
				bucket.getFormat(), size);
	}

}

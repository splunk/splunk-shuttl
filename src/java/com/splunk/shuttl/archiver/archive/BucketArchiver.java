// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.archive;

import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Archives buckets the way that it is configured to archive them.
 */
public class BucketArchiver implements BucketShuttler {

	private final BucketDeleter bucketDeleter;
	private final BucketCopier bucketCopier;

	public BucketArchiver(BucketCopier bucketCopier, BucketDeleter bucketDeleter) {
		this.bucketCopier = bucketCopier;
		this.bucketDeleter = bucketDeleter;
	}

	public void archiveBucket(LocalBucket bucket) {
		bucketCopier.copyBucket(bucket);
		bucketDeleter.deleteBucket(bucket);
	}

	@Override
	public void shuttlBucket(LocalBucket bucket) {
		archiveBucket(bucket);
	}
}

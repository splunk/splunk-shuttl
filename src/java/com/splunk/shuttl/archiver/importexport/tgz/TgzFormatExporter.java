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
package com.splunk.shuttl.archiver.importexport.tgz;

import java.io.File;

import com.splunk.shuttl.archiver.importexport.BucketExporter;
import com.splunk.shuttl.archiver.importexport.BucketFileCreator;
import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Changes the format of a bucket to gzip.
 */
public class TgzFormatExporter implements BucketExporter {

	private CreatesBucketTgz createsBucketTgz;
	private BucketFileCreator bucketFileCreator;

	/**
	 * @param createsBucketTgz
	 * @param bucketFileCreator
	 */
	public TgzFormatExporter(CreatesBucketTgz createsBucketTgz,
			BucketFileCreator bucketFileCreator) {
		this.createsBucketTgz = createsBucketTgz;
		this.bucketFileCreator = bucketFileCreator;
	}

	@Override
	public LocalBucket exportBucket(LocalBucket b) {
		File tgz = createsBucketTgz.createTgz(b);
		return bucketFileCreator.createBucketWithFile(tgz, b);
	}

	public static TgzFormatExporter create(CreatesBucketTgz createsBucketTgz) {
		return new TgzFormatExporter(createsBucketTgz,
				BucketFileCreator.createForTgz());
	}
}

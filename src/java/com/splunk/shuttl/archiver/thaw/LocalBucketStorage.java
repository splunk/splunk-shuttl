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

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Responsible for keeping track of the local buckets.
 */
public class LocalBucketStorage {

	private final ThawLocationProvider thawLocationProvider;

	/**
	 * @param thawLocationProvider
	 */
	public LocalBucketStorage(ThawLocationProvider thawLocationProvider) {
		this.thawLocationProvider = thawLocationProvider;
	}

	/**
	 * @param bucket
	 * @return
	 */
	public boolean hasBucket(Bucket bucket) {
		try {
			File thawLocation = thawLocationProvider
					.getLocationInThawForBucket(bucket);
			return thawLocation != null && thawLocation.exists();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}

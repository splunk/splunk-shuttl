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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.Bucket;

/**
 * Deletes {@link Bucket}s.
 */
public class BucketDeleter {

	private final Logger logger;

	public BucketDeleter(Logger logger) {
		this.logger = logger;
	}

	/**
	 * @param bucket
	 *          to delete.
	 */
	public void deleteBucket(Bucket bucket) {
		try {
			bucket.deleteBucket();
		} catch (IOException e) {
			logAndIgnoreDeletionException(bucket, e);
		}
	}

	private void logAndIgnoreDeletionException(Bucket bucket, IOException e) {
		logger.warn(warn("Deleted a bucket from local file system, "
				+ "because archiving was complete.", e, "Will ignore this exception",
				"bucket", bucket, "exception", e));
	}

	/**
	 * @return
	 */
	public static BucketDeleter create() {
		return new BucketDeleter(Logger.getLogger(BucketDeleter.class));
	}

}

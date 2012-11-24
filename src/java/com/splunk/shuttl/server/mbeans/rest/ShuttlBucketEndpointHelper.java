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
package com.splunk.shuttl.server.mbeans.rest;

import static com.splunk.shuttl.ShuttlConstants.*;
import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.File;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.BucketShuttler;
import com.splunk.shuttl.archiver.archive.BucketShuttlerRunner;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLock;
import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;

public class ShuttlBucketEndpointHelper {

	private static final Logger logger = Logger
			.getLogger(ShuttlBucketEndpointHelper.class);

	public static interface ShuttlProvider {
		BucketShuttler createWithConfig(ArchiveConfiguration config);
	}

	public static interface ConfigProvider {
		ArchiveConfiguration createWithBucket(LocalBucket bucket);
	}

	public static interface BucketModifier {
		LocalBucket modifyLocalBucket(LocalBucket bucket);
	}

	public static void shuttlBucket(String path, String index,
			ShuttlProvider shuttlProvider, ConfigProvider configProvider,
			BucketModifier bucketModifier) {
		verifyPathAndIndex(path, index);

		logEndpoint(path, index);
		try {
			LocalBucket bucket = createBucket(path, index);
			BucketLock bucketLock = createBucketLock(bucket);

			ArchiveConfiguration config = configProvider.createWithBucket(bucket);
			BucketShuttler bucketShuttler = shuttlProvider.createWithConfig(config);

			bucket = bucketModifier.modifyLocalBucket(bucket);

			Runnable r = new BucketShuttlerRunner(bucketShuttler, bucket, bucketLock);
			new Thread(r).run();
		} catch (Throwable e) {
			logAndThrowException(path, index, e);
		}
	}

	private static void logAndThrowException(String path, String index,
			Throwable e) {
		logger.error(did("Tried archiving a bucket", e, "To archive the bucket",
				"index", index, "bucket_path", path));
		throw new RuntimeException(e);
	}

	private static void logEndpoint(String path, String index) {
		logger.info(happened("Received REST request to copy bucket", "endpoint",
				ENDPOINT_BUCKET_COPY, "index", index, "path", path));
		logger.info(will("Attempting to archive bucket", "index", index, "path",
				path));
	}

	private static LocalBucket createBucket(String path, String index) {
		return BucketFactory.createBucketWithIndexDirectoryAndFormat(index,
				new File(path), BucketFormat.SPLUNK_BUCKET);
	}

	private static BucketLock createBucketLock(LocalBucket bucket) {
		BucketLock bucketLock = new ArchiveBucketLock(bucket);
		if (!bucketLock.tryLockShared())
			throw new IllegalStateException("We must ensure that the"
					+ " bucket archiver has a " + "lock to the bucket it will transfer");
		return bucketLock;
	}

	private static void verifyPathAndIndex(String path, String index) {
		if (path == null) {
			logger.error(happened("No path was provided."));
			throw new IllegalArgumentException("path must be specified");
		}
		if (index == null) {
			logger.error(happened("No index was provided."));
			throw new IllegalArgumentException("index must be specified");
		}
	}
}

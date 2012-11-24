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

public class ShuttlBucketEndpoint {

	public static interface ShuttlProvider {
		BucketShuttler createWithConfig(ArchiveConfiguration config);
	}

	public static interface ConfigProvider {
		ArchiveConfiguration createWithBucket(LocalBucket bucket);
	}

	public static interface BucketModifier {
		LocalBucket modifyLocalBucket(LocalBucket bucket);
	}

	private static final Logger logger = Logger
			.getLogger(ShuttlBucketEndpoint.class);

	private ShuttlProvider shuttlProvider;
	private ConfigProvider configProvider;
	private BucketModifier bucketModifier;
	private BucketFactory bucketFactory;

	public ShuttlBucketEndpoint(ShuttlProvider shuttlProvider,
			ConfigProvider configProvider, BucketModifier bucketModifier,
			BucketFactory bucketFactory) {
		this.shuttlProvider = shuttlProvider;
		this.configProvider = configProvider;
		this.bucketModifier = bucketModifier;
		this.bucketFactory = bucketFactory;
	}

	public void shuttlBucket(String path, String index) {
		verifyPathAndIndex(path, index);
		try {
			createAndRunBucketShuttling(path, index);
		} catch (Throwable e) {
			logger.error(did("Tried archiving a bucket", e, "To archive the bucket",
					"index", index, "bucket_path", path));
			throw new RuntimeException(e);
		}
	}

	private void verifyPathAndIndex(String path, String index) {
		if (path == null) {
			logger.error(happened("No path was provided."));
			throw new IllegalArgumentException("path must be specified");
		}
		if (index == null) {
			logger.error(happened("No index was provided."));
			throw new IllegalArgumentException("index must be specified");
		}
	}

	private void createAndRunBucketShuttling(String path, String index) {
		LocalBucket bucket = createBucket(path, index);
		BucketLock bucketLock = createBucketLock(bucket);
		BucketShuttler bucketShuttler = createShuttler(bucket);
		bucket = bucketModifier.modifyLocalBucket(bucket);

		runShuttlerOnASeparateThread(bucket, bucketLock, bucketShuttler);
	}

	private void runShuttlerOnASeparateThread(LocalBucket bucket,
			BucketLock bucketLock, BucketShuttler bucketShuttler) {
		Runnable r = new BucketShuttlerRunner(bucketShuttler, bucket, bucketLock);
		new Thread(r).run();
	}

	private BucketShuttler createShuttler(LocalBucket bucket) {
		ArchiveConfiguration config = configProvider.createWithBucket(bucket);
		BucketShuttler bucketShuttler = shuttlProvider.createWithConfig(config);
		return bucketShuttler;
	}

	private LocalBucket createBucket(String path, String index) {
		return bucketFactory.createWithIndexDirectoryAndFormat(index,
				new File(path), BucketFormat.SPLUNK_BUCKET);
	}

	private BucketLock createBucketLock(LocalBucket bucket) {
		BucketLock bucketLock = new ArchiveBucketLock(bucket);
		if (!bucketLock.tryLockShared())
			throw new IllegalStateException("We must ensure that the"
					+ " bucket archiver has a " + "lock to the bucket it will transfer");
		return bucketLock;
	}

}

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

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketArchiverRunner;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLock;
import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.clustering.GetsServerNameForReplicatedBucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_ARCHIVER)
public class ArchiveBucketEndpoint {

	private static final org.apache.log4j.Logger logger = Logger
			.getLogger(ArchiveBucketEndpoint.class);

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public void archiveBucket(@FormParam("path") String path,
			@FormParam("index") String index) {
		verifyValidArguments(path, index);
		logArchiveEndpoint(path, index);
		try {
			archiveBucketOnAnotherThread(index, path);
		} catch (Throwable e) {
			logger.error(did("Tried archiving a bucket", e, "To archive the bucket",
					"index", index, "bucket_path", path));
			throw new RuntimeException(e);
		}
	}

	private void verifyValidArguments(String path, String index) {
		if (path == null) {
			logger.error(happened("No path was provided."));
			throw new IllegalArgumentException("path must be specified");
		}
		if (index == null) {
			logger.error(happened("No index was provided."));
			throw new IllegalArgumentException("index must be specified");
		}
	}

	private void logArchiveEndpoint(String path, String index) {
		logMetricsAtEndpoint(ENDPOINT_BUCKET_ARCHIVER);

		logger.info(happened("Received REST request to archive bucket", "endpoint",
				ENDPOINT_BUCKET_ARCHIVER, "index", index, "path", path));
	}

	private void logMetricsAtEndpoint(String endpoint) {
		String logMessage = String.format(
				" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
				ENDPOINT_ARCHIVER, endpoint);
		logger.info(logMessage);
	}

	private void archiveBucketOnAnotherThread(String index, String path) {

		logger.info(will("Attempting to archive bucket", "index", index, "path",
				path));
		Runnable r = createBucketArchiverRunner(index, path);
		new Thread(r).run();
	}

	private Runnable createBucketArchiverRunner(String index, String path) {
		LocalBucket bucket = BucketFactory.createBucketWithIndexDirectoryAndFormat(
				index, new File(path), BucketFormat.SPLUNK_BUCKET);
		BucketLock bucketLock = new ArchiveBucketLock(bucket);
		throwExceptionIfSharedLockCannotBeAcquired(bucketLock);

		ArchiveConfiguration conf = getConfigurationDependingOnBucketProperties(bucket);
		BucketArchiver bucketArchiver = BucketArchiverFactory
				.createWithConfig(conf);
		return new BucketArchiverRunner(bucketArchiver, bucket, bucketLock);
	}

	private ArchiveConfiguration getConfigurationDependingOnBucketProperties(
			LocalBucket bucket) {
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		if (bucket.isReplicatedBucket())
			return configWithChangedServerNameForReplicatedBucket(bucket, config);
		else
			return config;
	}

	private ArchiveConfiguration configWithChangedServerNameForReplicatedBucket(
			LocalBucket bucket, ArchiveConfiguration configuration) {
		String serverName = GetsServerNameForReplicatedBucket.create()
				.getServerName(bucket);
		return configuration.newConfigWithServerName(serverName);
	}

	private void throwExceptionIfSharedLockCannotBeAcquired(BucketLock bucketLock) {
		if (!bucketLock.tryLockShared())
			throw new IllegalStateException("We must ensure that the"
					+ " bucket archiver has a " + "lock to the bucket it will transfer");
	}
}

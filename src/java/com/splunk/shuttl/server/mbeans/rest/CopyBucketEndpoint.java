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
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketShuttlerRunner;
import com.splunk.shuttl.archiver.archive.BucketCopier;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.recovery.ArchiveBucketLock;
import com.splunk.shuttl.archiver.bucketlock.BucketLock;
import com.splunk.shuttl.archiver.clustering.GetsServerNameForReplicatedBucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_COPY)
public class CopyBucketEndpoint {

	private static final Logger logger = Logger
			.getLogger(CopyBucketEndpoint.class);

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public void copyBucket(@FormParam("path") String path,
			@FormParam("index") String index) {
		verifyPathAndIndex(path, index);

		logger.info(happened("Received REST request to copy bucket", "endpoint",
				ENDPOINT_BUCKET_COPY, "index", index, "path", path));
		try {
			logger.info(will("Attempting to archive bucket", "index", index, "path",
					path));
			LocalBucket bucket = BucketFactory
					.createBucketWithIndexDirectoryAndFormat(index, new File(path),
							BucketFormat.SPLUNK_BUCKET);
			BucketLock bucketLock = new ArchiveBucketLock(bucket);
			if (!bucketLock.tryLockShared())
				throw new IllegalStateException("We must ensure that the"
						+ " bucket archiver has a " + "lock to the bucket it will transfer");
			ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
			if (bucket.isReplicatedBucket()) {
				String serverName = GetsServerNameForReplicatedBucket.create()
						.getServerName(bucket);
				config = config.newConfigWithServerName(serverName);
			}

			BucketCopier bucketCopier = BucketArchiverFactory
					.createCopierWithConfig(config);

			LocalBucket b;
			if (bucket.isReplicatedBucket()) {
				String normalizedBucketName = bucket.getName().replaceFirst("rb", "db");
				b = BucketFactory.createBucketWithIndexDirectoryBucketNameAndSize(
						bucket.getIndex(), new File(bucket.getPath()),
						normalizedBucketName, bucket.getFormat(), bucket.getSize());
			} else
				b = bucket;

			bucket = b;
			Runnable r = new BucketShuttlerRunner(bucketCopier, bucket, bucketLock);
			new Thread(r).run();
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
}

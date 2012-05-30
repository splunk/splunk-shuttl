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

import java.io.FileNotFoundException;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketArchiver;
import com.splunk.shuttl.archiver.archive.BucketArchiverFactory;
import com.splunk.shuttl.archiver.archive.BucketArchiverRunner;
import com.splunk.shuttl.archiver.archive.recovery.BucketLock;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.metrics.ShuttlMetricsHelper;

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
		archiveBucketOnAnotherThread(index, path);
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
		ShuttlMetricsHelper.update(logger, logMessage);
	}

	private void archiveBucketOnAnotherThread(String index, String path) {

		logger.info(will("Attempting to archive bucket", "index", index, "path",
				path));
		Runnable r = createBucketArchiverRunner(index, path);
		new Thread(r).run();
	}

	private Runnable createBucketArchiverRunner(String indexName, String path) {
		BucketArchiver bucketArchiver = BucketArchiverFactory
				.createConfiguredArchiver();
		Bucket bucket = createBucketWithErrorHandling(indexName, path);
		BucketLock bucketLock = new BucketLock(bucket);
		if (!bucketLock.tryLockShared())
			throw new IllegalStateException(
					"We must ensure that the bucket archiver has a "
							+ "lock to the bucket it will transfer");
		Runnable r = new BucketArchiverRunner(bucketArchiver, bucket, bucketLock);
		return r;
	}

	private Bucket createBucketWithErrorHandling(String indexName, String path) {
		Bucket bucket;
		try {
			bucket = new Bucket(indexName, path);
		} catch (FileNotFoundException e) {
			logger.error(did(
					"attempted to create bucket object from existing bucket directory",
					"bucket directory did not exist", "existing bucket directory",
					"path", path, "index name ", indexName));
			throw new RuntimeException(e);
		} catch (FileNotDirectoryException e) {
			logger.error(did(
					"attempted to create bucket object from existing bucket",
					"specified path was a file", "specified path to be a directory",
					"path", path, "index name ", indexName));
			throw new RuntimeException(e);
		}
		return bucket;
	}

}

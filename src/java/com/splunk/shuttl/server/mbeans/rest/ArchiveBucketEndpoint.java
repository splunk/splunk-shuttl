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
import java.io.FileFilter;
import java.io.FileNotFoundException;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.archive.BucketShuttler;
import com.splunk.shuttl.archiver.archive.BucketShuttlerFactory;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.BucketModifier;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ShuttlProvider;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_ARCHIVER)
public class ArchiveBucketEndpoint {

	private static Logger logger = Logger.getLogger(ArchiveBucketEndpoint.class);

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public void archiveBucket(@FormParam("path") String path,
			@FormParam("index") String index) {
		try {
			ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();

			if (isPathReplicatedBucketWithRawdataOnly(path)) {
				deletePath(path);
				logReason(path, index);
			} else {
				ShuttlBucketEndpointHelper.shuttlBucket(path, index,
						new BucketArchiverProvider(),
						ConfigProviderForBothNormalAndReplicatedBuckets.create(config),
						new RenamesReplicatedBucketAsNormalBucket());
			}
		} catch (Throwable t) {
			logger.error(did("Tried archiving bucket", t, "to archive the bucket",
					"path", path, "index", index));
			throw new RuntimeException(t);
		}
	}

	private void logReason(String path, String index) {
		logger.warn(warn(
				"Tried shuttling a replicted bucket that only contained rawdata",
				"Will not Shuttl this bucket, because Shuttl does not support rawdata only"
						+ " buckets.",
				"Deleted bucket. But don't worry, searchable copies and the "
						+ "original bucket will still be Shuttled, by the other indexers. "
						+ "You have Shuttl installed at all indexers, don't you?",
				"bucket_path", path, "index", index));
	}

	private boolean isPathReplicatedBucketWithRawdataOnly(String path)
			throws FileNotFoundException, FileNotDirectoryException {
		LocalBucket localBucket = new LocalBucket(new File(path), "doesNotMatter",
				BucketFormat.UNKNOWN);

		if (localBucket.isReplicatedBucket())
			if (listFilesThatAreNotRawdataDirNorDotFiles(localBucket).length == 0)
				return true;
		return false;
	}

	private File[] listFilesThatAreNotRawdataDirNorDotFiles(
			LocalBucket localBucket) {
		return localBucket.getDirectory().listFiles(new FileFilter() {

			@Override
			public boolean accept(File f) {
				if (isRawdataOrDotFile(f.getName()))
					return false;
				return true;
			}

			private boolean isRawdataOrDotFile(String fileName) {
				return fileName.equals("rawdata") || fileName.startsWith(".");
			}
		});
	}

	private void deletePath(String path) {
		new File(path).delete();
	}

	private static class BucketArchiverProvider implements ShuttlProvider {

		@Override
		public BucketShuttler createWithConfig(ArchiveConfiguration config) {
			return BucketShuttlerFactory.createWithConfig(config);
		}
	}

	private static class RenamesReplicatedBucketAsNormalBucket implements
			BucketModifier {

		@Override
		public LocalBucket modifyLocalBucket(LocalBucket bucket) {
			return getNormalizedBucket(bucket);
		}

		private LocalBucket getNormalizedBucket(LocalBucket bucket) {
			if (bucket.isReplicatedBucket())
				return getBucketWithNormalBucketName(bucket);
			else
				return bucket;
		}

		private LocalBucket getBucketWithNormalBucketName(LocalBucket b) {
			String normalizedBucketName = b.getName().replaceFirst("rb", "db");
			return BucketFactory.createBucketWithIndexDirectoryBucketNameAndSize(
					b.getIndex(), new File(b.getPath()), normalizedBucketName,
					b.getFormat(), b.getSize());
		}
	}
}

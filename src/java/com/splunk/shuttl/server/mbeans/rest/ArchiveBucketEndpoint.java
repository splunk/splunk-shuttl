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

import java.io.File;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketShuttler;
import com.splunk.shuttl.archiver.archive.BucketShuttlerFactory;
import com.splunk.shuttl.archiver.clustering.GetsServerNameForReplicatedBucket;
import com.splunk.shuttl.archiver.model.BucketFactory;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.BucketModifier;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ConfigProvider;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ShuttlProvider;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_ARCHIVER)
public class ArchiveBucketEndpoint {

	@POST
	@Produces(MediaType.TEXT_PLAIN)
	public void archiveBucket(@FormParam("path") String path,
			@FormParam("index") String index) {
		ShuttlBucketEndpointHelper.shuttlBucket(path, index,
				new BucketArchiverProvider(),
				new ConfigProviderForBothNormalAndReplicatedBuckets(),
				new RenamesReplicatedBucketAsNormalBucket());
	}

	private static class BucketArchiverProvider implements ShuttlProvider {

		@Override
		public BucketShuttler createWithConfig(ArchiveConfiguration config) {
			return BucketShuttlerFactory.createWithConfig(config);
		}
	}

	private static class ConfigProviderForBothNormalAndReplicatedBuckets
			implements ConfigProvider {

		@Override
		public ArchiveConfiguration createWithBucket(LocalBucket bucket) {
			return getConfigurationDependingOnBucketProperties(bucket);
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

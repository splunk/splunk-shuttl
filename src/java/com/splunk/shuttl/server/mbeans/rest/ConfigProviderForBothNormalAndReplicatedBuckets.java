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

import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.clustering.GetsServerNameForBucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.rest.ShuttlBucketEndpoint.ConfigProvider;

/**
 * Provides an {@link ArchiveConfiguration}. <br/>
 * In addition it deletes the bucket, in case anything goes wrong with getting
 * the server name for a replicated bucket.
 */
public class ConfigProviderForBothNormalAndReplicatedBuckets implements
		ConfigProvider {

	private static final String replicatedBucketDeletionExplanation = "will delete "
			+ "replicated bucket to free up space. Shuttling replicated buckets will work "
			+ "again when network issues have been fixed. The original bucket will still "
			+ "be shuttl'ed";

	private static final Logger logger = Logger
			.getLogger(ConfigProviderForBothNormalAndReplicatedBuckets.class);

	private final ArchiveConfiguration config;
	private final GetsServerNameForBucket getsServerNameForBucket;
	private final BucketDeleter bucketDeleter;

	public ConfigProviderForBothNormalAndReplicatedBuckets(
			ArchiveConfiguration config,
			GetsServerNameForBucket getsServerNameForBucket,
			BucketDeleter bucketDeleter) {
		this.config = config;
		this.getsServerNameForBucket = getsServerNameForBucket;
		this.bucketDeleter = bucketDeleter;
	}

	@Override
	public ArchiveConfiguration createWithBucket(LocalBucket bucket) {
		return getConfigurationDependingOnBucketProperties(bucket);
	}

	private ArchiveConfiguration getConfigurationDependingOnBucketProperties(
			LocalBucket bucket) {
		if (bucket.isReplicatedBucket())
			return configWithChangedServerNameForReplicatedBucket(bucket, config);
		else
			return config;
	}

	private ArchiveConfiguration configWithChangedServerNameForReplicatedBucket(
			LocalBucket bucket, ArchiveConfiguration configuration) {
		try {
			String serverName = getsServerNameForBucket.getServerName(bucket);
			return configuration.newConfigWithServerName(serverName);
		} catch (Exception e) {
			logDeletionOfReplicatedBucket(bucket, e);
			throw new RuntimeException(e);
		}
	}

	private void logDeletionOfReplicatedBucket(LocalBucket bucket, Exception e) {
		bucketDeleter.deleteBucket(bucket);
		logger.warn(warn(
				"Tried getting configured server name from remote shuttl server", e,
				replicatedBucketDeletionExplanation, "bucket", bucket));
	}

	public static ConfigProviderForBothNormalAndReplicatedBuckets create(
			ArchiveConfiguration config) {
		return new ConfigProviderForBothNormalAndReplicatedBuckets(config,
				GetsServerNameForBucket.create(config), BucketDeleter.create());
	}
}

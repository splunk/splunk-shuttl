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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.BucketDeleter;
import com.splunk.shuttl.archiver.clustering.GetsServerNameForBucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class ConfigProviderForBothNormalAndReplicatedBucketsTest {

	private ConfigProviderForBothNormalAndReplicatedBuckets configProvider;
	private GetsServerNameForBucket getsServerNameForBucket;
	private ArchiveConfiguration config;
	private BucketDeleter bucketDeleter;

	@BeforeMethod
	public void setUp() {
		config = mock(ArchiveConfiguration.class);
		getsServerNameForBucket = mock(GetsServerNameForBucket.class);
		bucketDeleter = mock(BucketDeleter.class);
		configProvider = new ConfigProviderForBothNormalAndReplicatedBuckets(
				config, getsServerNameForBucket, bucketDeleter);
	}

	public void createWithBucket_bucketIsNotAReplicatedBucket_returnsNonModifiedConfig() {
		LocalBucket bucket = TUtilsBucket.createBucket();
		assertFalse(bucket.isReplicatedBucket());
		ArchiveConfiguration actualConfig = configProvider.createWithBucket(bucket);
		assertEquals(config, actualConfig);
	}

	public void createWithBucket_replicatedBucket_configWithServerNameForReplicatedBucket() {
		LocalBucket replicatedBucket = TUtilsBucket.createReplicatedBucket();
		String serverName = "serverName";
		when(getsServerNameForBucket.getServerName(replicatedBucket)).thenReturn(
				serverName);
		ArchiveConfiguration configWithServerName = mock(ArchiveConfiguration.class);
		when(config.newConfigWithServerName(serverName)).thenReturn(
				configWithServerName);

		ArchiveConfiguration actualConfig = configProvider
				.createWithBucket(replicatedBucket);
		assertEquals(actualConfig, configWithServerName);
	}

	public void createWithBucket_replicatedBucketAndGetsServerNameThrows_deletesBucketAndThrows() {
		LocalBucket replicatedBucket = TUtilsBucket.createReplicatedBucket();
		when(getsServerNameForBucket.getServerName(replicatedBucket)).thenThrow(
				new RuntimeException());

		try {
			configProvider.createWithBucket(replicatedBucket);
			fail();
		} catch (RuntimeException e) {
		}

		verify(bucketDeleter).deleteBucket(replicatedBucket);
	}

	public void createWithBucket_bucketIsNotAReplicatedBucket_getsServerNameForBucketIsNotCalled() {
		LocalBucket bucket = TUtilsBucket.createBucket();
		configProvider.createWithBucket(bucket);
		verifyZeroInteractions(getsServerNameForBucket);
	}
}

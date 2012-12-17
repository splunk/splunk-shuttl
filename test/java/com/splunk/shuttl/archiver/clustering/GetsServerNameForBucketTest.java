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
package com.splunk.shuttl.archiver.clustering;

import static org.mockito.Matchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.model.Bucket;

@Test(groups = { "fast-unit" })
public class GetsServerNameForBucketTest {

	private GetsServerNameForBucket underTest;
	private ClusterMaster clusterMaster;
	private Bucket replicatedBucket;
	private CallsRemoteIndexer remoteIndexer;
	private ArchiveConfiguration config;

	@BeforeMethod
	public void setUp() {
		clusterMaster = mock(ClusterMaster.class);
		remoteIndexer = mock(CallsRemoteIndexer.class);
		config = mock(ArchiveConfiguration.class);
		underTest = new GetsServerNameForBucket(clusterMaster, remoteIndexer,
				config);
		replicatedBucket = mock(Bucket.class);
		when(replicatedBucket.isReplicatedBucket()).thenReturn(true);
	}

	public void _nonReplicatedBucket_returnsArchiveConfigsServerName() {
		String expected = "serverName";
		when(config.getServerName()).thenReturn(expected);
		Bucket bucket = mock(Bucket.class);
		when(bucket.isReplicatedBucket()).thenReturn(false);

		assertEquals(underTest.getServerName(bucket), expected);
	}

	public void _replicatedBucket_asksClusterMasterForTheIndexerGUIDTheReplicatedBucketCameFrom() {
		String guid = "guid";
		when(replicatedBucket.getGuid()).thenReturn(guid);

		underTest.getServerName(replicatedBucket);
		verify(clusterMaster).indexerForGuid(guid);
	}

	public void _indexerInfo_askRemoteIndexerForShuttlsConfiguredServerName() {
		String serverName = "serverName";
		IndexerInfo indexerInfo = mock(IndexerInfo.class);
		when(clusterMaster.indexerForGuid(anyString())).thenReturn(indexerInfo);
		when(remoteIndexer.getShuttlConfiguredServerName(indexerInfo)).thenReturn(
				serverName);
		String actual = underTest.getServerName(replicatedBucket);
		assertEquals(actual, serverName);
	}

}

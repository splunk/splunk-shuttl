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

import java.net.URI;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.ClusterConfig;
import com.splunk.ClusterPeer;
import com.splunk.ClusterPeers;
import com.splunk.Service;
import com.splunk.shuttl.archiver.clustering.ClusterMaster.ClusterConfigException;
import com.splunk.shuttl.archiver.clustering.ClusterMaster.ClusterPeersException;

@Test(groups = { "fast-unit" })
public class ClusterMasterTest {

	private ClusterMaster clusterMaster;
	private ClusterConfig clusterConfig;
	private String guid;
	private ClusterPeersProvider peersProvider;

	@BeforeMethod
	public void setUp() {
		clusterConfig = mock(ClusterConfig.class, RETURNS_MOCKS);
		peersProvider = mock(ClusterPeersProvider.class, RETURNS_MOCKS);
		clusterMaster = new ClusterMaster(clusterConfig, peersProvider);
		guid = "guid";
	}

	public void getIndexerForGuid_clusterPeers_createsIndexerInfoWithClusterPeer() {
		String host = "host";
		int port = 1234;
		URI masterUri = URI.create("https://" + host + ":" + port);

		when(clusterConfig.getClusterMasterUri()).thenReturn(masterUri);
		ClusterPeers clusterPeers = mock(ClusterPeers.class);
		when(peersProvider.getClusterPeers(masterUri)).thenReturn(clusterPeers);
		ClusterPeer clusterPeer = mock(ClusterPeer.class);
		when(clusterPeers.get(guid)).thenReturn(clusterPeer);
		when(clusterPeer.getHost()).thenReturn(host);
		when(clusterPeer.getPort()).thenReturn(port);

		IndexerInfo indexer = clusterMaster.indexerForGuid(guid);
		assertEquals(indexer, new IndexerInfo(host, port));
	}

	@Test(expectedExceptions = { ClusterConfigException.class })
	public void getIndexerForGuid_clusterConfigReturnsNull_throws() {
		when(clusterConfig.getClusterMasterUri()).thenReturn(null);
		clusterMaster.indexerForGuid(guid);
	}

	@Test(expectedExceptions = { ClusterPeersException.class })
	public void getIndexerForGuid_clusterPeersReturnsNull_throws() {
		stubNonNullClusterMasterUriToAvoidClusterConfigException();

		ClusterPeers clusterPeers = mock(ClusterPeers.class);
		stubSplunkServiceForExceptionMessage(clusterPeers);
		when(peersProvider.getClusterPeers(any(URI.class)))
				.thenReturn(clusterPeers);

		when(clusterPeers.get(guid)).thenReturn(null);
		clusterMaster.indexerForGuid(guid);
	}

	private void stubSplunkServiceForExceptionMessage(ClusterPeers clusterPeers) {
		when(clusterPeers.getService()).thenReturn(mock(Service.class));
	}

	private void stubNonNullClusterMasterUriToAvoidClusterConfigException() {
		when(clusterConfig.getClusterMasterUri()).thenReturn(
				URI.create("valid:/uri"));
	}
}

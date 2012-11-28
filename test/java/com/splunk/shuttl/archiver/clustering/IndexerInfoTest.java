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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.ClusterPeer;
import com.splunk.shuttl.archiver.clustering.IndexerInfo.ClusterPeerException;

@Test(groups = { "fast-unit" })
public class IndexerInfoTest {

	private String host;
	private int port;
	private IndexerInfo indexerInfo;

	@BeforeMethod
	public void setUp() {
		host = "host";
		port = 1234;
		indexerInfo = new IndexerInfo(host, port);
	}

	private void assertGetHostEqualsSetUpHost(IndexerInfo instance) {
		assertEquals(instance.getHost(), host);
	}

	private void assertGetPortEqualsSetUpHost(IndexerInfo instance) {
		assertEquals(instance.getPort(), port);
	}

	public void getHost_givenHost_returnsHost() {
		assertGetHostEqualsSetUpHost(indexerInfo);
	}

	public void getPort_givenPort_returnsPort() {
		assertGetPortEqualsSetUpHost(indexerInfo);
	}

	public void create_givenClusterPeer_hostAndPort() {
		ClusterPeer peer = mock(ClusterPeer.class);
		when(peer.getHost()).thenReturn(host);
		when(peer.getPort()).thenReturn(port);
		IndexerInfo instance = IndexerInfo.create(peer);
		assertGetHostEqualsSetUpHost(instance);
		assertGetPortEqualsSetUpHost(instance);
	}

	@Test(expectedExceptions = { ClusterPeerException.class })
	public void create_clusterPeerHostIsNull_throws() {
		ClusterPeer peer = mock(ClusterPeer.class);
		when(peer.getHost()).thenReturn(null);
		IndexerInfo.create(peer);
	}

	@Test(expectedExceptions = { ClusterPeerException.class })
	public void create_clusterPeerPortIsNull_throws() {
		ClusterPeer peer = mock(ClusterPeer.class);
		when(peer.getHost()).thenReturn(host);
		when(peer.getPort()).thenReturn(null);
		IndexerInfo.create(peer);
	}
}

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

import java.net.URI;

import com.splunk.ClusterConfig;
import com.splunk.ClusterPeer;
import com.splunk.ClusterPeers;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;

/**
 * Handles all communication to the ClusterMaster.
 */
public class ClusterMaster {

	private final ClusterConfig clusterConfig;
	private ClusterPeersProvider peersProvider;

	public ClusterMaster(ClusterConfig clusterConfig,
			ClusterPeersProvider peersProvider) {
		this.clusterConfig = clusterConfig;
		this.peersProvider = peersProvider;
	}

	/**
	 * Indexer host and port from a guid.
	 */
	public IndexerInfo indexerForGuid(String guid) {
		URI clusterMasterUri = getClusterMasterUriChecked();
		ClusterPeer clusterPeer = getClusterPeerChecked(guid, clusterMasterUri);
		return IndexerInfo.create(clusterPeer);
	}

	private URI getClusterMasterUriChecked() {
		URI clusterMasterUri = clusterConfig.getClusterMasterUri();
		if (clusterMasterUri == null)
			throwClusterConfigException();
		return clusterMasterUri;
	}

	private void throwClusterConfigException() {
		throw new ClusterConfigException(
				"Could not get cluster master uri from splunk indexer: "
						+ clusterConfig.getService().getHost());
	}

	private ClusterPeer getClusterPeerChecked(String guid, URI clusterMasterUri) {
		ClusterPeers clusterPeers = peersProvider.getClusterPeers(clusterMasterUri);
		ClusterPeer clusterPeer = clusterPeers.get(guid);
		if (clusterPeer == null)
			throwClusterPeersException(guid, clusterPeers);
		return clusterPeer;
	}

	private void throwClusterPeersException(String guid, ClusterPeers clusterPeers) {
		throw new ClusterPeersException("Could not find a cluster peer with guid: "
				+ guid + ", at cluster master: " + clusterPeers.getService().getHost());
	}

	public static ClusterMaster create() {
		return new ClusterMaster(new ClusterConfig(
				SplunkIndexedLayerFactory.getLoggedInSplunkService()),
				ClusterPeersProvider.create());
	}

	public static class ClusterConfigException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ClusterConfigException(String msg) {
			super(msg);
		}
	}

	public static class ClusterPeersException extends RuntimeException {
		private static final long serialVersionUID = 1L;

		public ClusterPeersException(String msg) {
			super(msg);
		}
	}

}

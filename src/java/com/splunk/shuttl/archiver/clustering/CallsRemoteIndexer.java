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

/**
 * Makes calls to a remote indexer.
 */
public class CallsRemoteIndexer {

	private final ShuttlPortEndpointProvider entityProvider;
	private final RemoteShuttl remoteShuttl;

	public CallsRemoteIndexer(ShuttlPortEndpointProvider entityProvider,
			RemoteShuttl remoteShuttl) {
		this.entityProvider = entityProvider;
		this.remoteShuttl = remoteShuttl;
	}

	public String getShuttlConfiguredServerName(IndexerInfo indexerInfo) {
		int shuttlPort = getShuttlPort(indexerInfo);
		return remoteShuttl.getServerName(indexerInfo.getHost(), shuttlPort);
	}

	private int getShuttlPort(IndexerInfo indexerInfo) {
		return entityProvider.getForIndexerInfo(indexerInfo).getShuttlPort();
	}

	public static CallsRemoteIndexer create() {
		return new CallsRemoteIndexer(new ShuttlPortEndpointProvider(),
				RemoteShuttl.create());
	}
}

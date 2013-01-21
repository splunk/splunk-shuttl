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
package com.splunk.shuttl.server.distributed;

import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

import com.amazonaws.util.json.JSONObject;
import com.splunk.DistributedPeer;
import com.splunk.EntityCollection;
import com.splunk.Service;
import com.splunk.shuttl.archiver.clustering.ShuttlPortEndpoint;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.thaw.SplunkConfiguration;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;

/**
 * Makes requests on the distributed peers connected to the Shuttl's Splunk.
 */
public abstract class RequestOnSearchPeers {

	protected String endpoint;
	protected String index;
	protected String from;
	protected String to;

	public RequestOnSearchPeers(String endpoint, String index, String from,
			String to) {
		this.endpoint = endpoint;
		this.index = index;
		this.from = from;
		this.to = to;
	}

	/**
	 * @return JSONObjects as response from each distributed peer.
	 */
	public List<JSONObject> execute() {
		return requestOnSearchPeers();
	}

	private List<JSONObject> requestOnSearchPeers() {
		Service splunkService = SplunkIndexedLayerFactory
				.getLoggedInSplunkService();
		EntityCollection<DistributedPeer> distributedPeers = splunkService
				.getDistributedPeers();

		return jsonResponsesFromExecutedRequestOnPeers(distributedPeers);
	}

	private List<JSONObject> jsonResponsesFromExecutedRequestOnPeers(
			EntityCollection<DistributedPeer> distributedPeers) {
		List<JSONObject> jsons = new ArrayList<JSONObject>();
		if (distributedPeers != null)
			for (DistributedPeer dp : distributedPeers.values())
				jsons.add(executeRequestOnPeers(dp));
		return jsons;
	}

	private JSONObject executeRequestOnPeers(DistributedPeer dp) {
		JsonRestEndpointCaller endpointCaller = new JsonRestEndpointCaller(
				new DefaultHttpClient());

		Service dpService = getDistributedPeerService(dp);
		int shuttlPort = getShuttlPort(dpService);
		return endpointCaller.getJson(createRequest(dpService, shuttlPort));
	}

	private Service getDistributedPeerService(DistributedPeer dp) {
		Service dpService = createService(dp);
		authenticateService(dpService);
		return dpService;
	}

	private Service createService(DistributedPeer dp) {
		String nameThatIsThePeersIpAndPortPair = dp.getName();
		String[] hostPortPair = nameThatIsThePeersIpAndPortPair.split(":");
		return new Service(hostPortPair[0], Integer.parseInt(hostPortPair[1]));
	}

	private void authenticateService(Service dpService) {
		SplunkConfiguration splunkConf = SplunkConfiguration.create();
		dpService.login(splunkConf.getUsername(), splunkConf.getPassword());
	}

	private int getShuttlPort(Service dpService) {
		return ShuttlPortEndpoint.create(dpService).getShuttlPort();
	}

	protected abstract HttpUriRequest createRequest(Service dpService,
			int shuttlPort);

}

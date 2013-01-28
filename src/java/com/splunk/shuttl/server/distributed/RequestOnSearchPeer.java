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

import com.amazonaws.util.json.JSONObject;
import com.splunk.DistributedPeer;
import com.splunk.Service;
import com.splunk.shuttl.archiver.clustering.ShuttlPortEndpoint;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.thaw.SplunkConfiguration;

public class RequestOnSearchPeer {

	private final ShuttlEndpointRequestProvider requestProvider;
	private final JsonRestEndpointCaller endpointCaller;
	private final SplunkConfiguration splunkConf;

	public RequestOnSearchPeer(ShuttlEndpointRequestProvider requestProvider,
			JsonRestEndpointCaller endpointCaller,
			SplunkConfiguration splunkConfiguration) {
		this.requestProvider = requestProvider;
		this.endpointCaller = endpointCaller;
		this.splunkConf = splunkConfiguration;
	}

	public JSONObject executeRequest(DistributedPeer dp) {
		Service dpService = getDistributedPeerService(dp);
		int shuttlPort = ShuttlPortEndpoint.create(dpService).getShuttlPort();
		return endpointCaller.getJson(requestProvider.createRequest(
				dpService.getHost(), shuttlPort));
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
		dpService.login(splunkConf.getUsername(), splunkConf.getPassword());
	}

}

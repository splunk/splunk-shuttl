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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONObject;
import com.splunk.DistributedPeer;
import com.splunk.EntityCollection;
import com.splunk.Service;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;
import com.splunk.shuttl.archiver.thaw.SplunkConfiguration;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;

/**
 * Makes requests on the distributed peers connected to the Shuttl's Splunk.
 */
public class RequestOnSearchPeers {

	private static final Logger logger = Logger
			.getLogger(RequestOnSearchPeers.class);

	private final Service splunkService;
	private final RequestOnSearchPeer requestOnSearchPeer;

	private final List<RuntimeException> exceptions;

	public RequestOnSearchPeers(Service splunkService,
			RequestOnSearchPeer requestOnSearchPeer) {
		this.splunkService = splunkService;
		this.requestOnSearchPeer = requestOnSearchPeer;
		this.exceptions = new LinkedList<RuntimeException>();
	}

	/**
	 * @return JSONObjects as response from each distributed peer.
	 */
	public List<JSONObject> execute() {
		return requestOnSearchPeers();
	}

	private List<JSONObject> requestOnSearchPeers() {
		List<JSONObject> jsons = new ArrayList<JSONObject>();
		exceptions.clear();

		EntityCollection<DistributedPeer> distributedPeers = splunkService
				.getDistributedPeers();
		if (distributedPeers != null)
			for (DistributedPeer dp : distributedPeers.values())
				executeRequestOnPeer(dp, jsons);
		return jsons;
	}

	private void executeRequestOnPeer(DistributedPeer dp, List<JSONObject> jsons) {
		try {
			jsons.add(requestOnSearchPeer.executeRequest(dp));
		} catch (RuntimeException e) {
			logger.warn(warn("Executed request on distributed peer", e,
					"will add to exceptions, which can be "
							+ "retrieved with getExceptions()"));
			exceptions.add(e);
		}
	}

	public List<RuntimeException> getExceptions() {
		return exceptions;
	}

	public static RequestOnSearchPeers createPost(String endpoint, String index,
			String from, String to) {
		return create(new PostRequestProvider(endpoint, index, from, to));
	}

	public static RequestOnSearchPeers createGet(String endpoint, String index,
			String from, String to) {
		return create(new GetRequestProvider(endpoint, index, from, to));
	}

	private static RequestOnSearchPeers create(
			ShuttlEndpointRequestProvider requestProvider) {
		Service splunkService = SplunkIndexedLayerFactory
				.getLoggedInSplunkService();
		return new RequestOnSearchPeers(splunkService, new RequestOnSearchPeer(
				requestProvider, JsonRestEndpointCaller.create(),
				SplunkConfiguration.create()));
	}
}

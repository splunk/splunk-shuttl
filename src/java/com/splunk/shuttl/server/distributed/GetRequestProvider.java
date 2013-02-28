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

import java.net.URI;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

import com.splunk.shuttl.server.mbeans.util.EndpointUtils;

/**
 * Creates a GET request for RequestOnSearchPeers
 */
public class GetRequestProvider implements ShuttlEndpointRequestProvider {

	private final String endpoint;
	private final String index;
	private final String from;
	private final String to;

	public GetRequestProvider(String endpoint, String index, String from,
			String to) {
		this.endpoint = endpoint;
		this.index = index;
		this.from = from;
		this.to = to;
	}

	@Override
	public HttpUriRequest createRequest(String shuttlHost, int shuttlPort) {
		URI endpointUri = EndpointUtils.getShuttlEndpointUri(shuttlHost,
				shuttlPort, endpoint);
		return new HttpGet(URI.create(endpointUri
				+ "?"
				+ EndpointUtils.createHttpGetParams("index", index, "from", from, "to",
						to)));
	}

}

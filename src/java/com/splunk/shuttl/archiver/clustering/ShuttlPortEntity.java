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

import org.apache.http.client.methods.HttpGet;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.Service;
import com.splunk.shuttl.archiver.http.InsecureHttpClientFactory;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;

/**
 * Calls splunk instances shuttl port.
 */
public class ShuttlPortEntity {

	private final Service service;
	private final JsonRestEndpointCaller restEndpointCaller;

	private ShuttlPortEntity(Service service,
			JsonRestEndpointCaller jsonRestEndpointCaller) {
		this.service = service;
		this.restEndpointCaller = jsonRestEndpointCaller;
	}

	/**
	 * @return the configured shuttl port of the Splunk instance which the service
	 *         is connected to.
	 */
	public int getShuttlPort() {
		HttpGet httpGet = createHttpGetRequest();
		JSONObject jsonObject = restEndpointCaller.getJson(httpGet);
		return getShuttlPortFromJSONResponse(jsonObject);
	}

	private HttpGet createHttpGetRequest() {
		URI shuttlPortRequestUri = URI
				.create(service.getScheme() + "://" + service.getHost() + ":"
						+ service.getPort() + "/services/shuttl/port");
		HttpGet httpGet = new HttpGet(shuttlPortRequestUri);
		return httpGet;
	}

	private int getShuttlPortFromJSONResponse(JSONObject jsonObject) {
		try {
			return Integer.parseInt(jsonObject.getString("shuttl_port"));
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	public static ShuttlPortEntity create(Service splunkService) {
		JsonRestEndpointCaller endpointCaller = new JsonRestEndpointCaller(
				InsecureHttpClientFactory.getInsecureHttpClient());
		return new ShuttlPortEntity(splunkService, endpointCaller);
	}
}

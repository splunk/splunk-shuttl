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

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.JSONTokener;
import com.splunk.shuttl.ShuttlConstants;

/**
 * Does REST calls to remote shuttls.
 */
public class RemoteShuttl {

	private DefaultHttpClient defaultHttpClient;

	/**
	 * @param defaultHttpClient
	 */
	public RemoteShuttl(DefaultHttpClient defaultHttpClient) {
		this.defaultHttpClient = defaultHttpClient;
	}

	/**
	 * @return server name from a Shuttl server.
	 */
	public String getServerName(String hostname, int shuttlPort) {
		HttpGet get = constructServerNameRequest(hostname, shuttlPort);
		return getServerNameByRestRequest(get);
	}

	private HttpGet constructServerNameRequest(String hostname, int shuttlPort) {
		return new HttpGet("http://" + hostname + ":" + shuttlPort + "/"
				+ ShuttlConstants.ENDPOINT_CONTEXT
				+ ShuttlConstants.ENDPOINT_SHUTTL_CONFIGURATION
				+ ShuttlConstants.ENDPOINT_CONFIG_SERVERNAME);
	}

	private String getServerNameByRestRequest(HttpGet httpGet) {
		HttpResponse response = executeRequest(httpGet);
		InputStream reponseContent = getReponseContent(response);
		return getServerNameFromResponse(reponseContent);
	}

	private HttpResponse executeRequest(HttpGet httpGet) {
		try {
			return defaultHttpClient.execute(httpGet);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private InputStream getReponseContent(HttpResponse response) {
		try {
			return response.getEntity().getContent();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private String getServerNameFromResponse(InputStream responseContent) {
		try {
			JSONObject jsonObject = createJsonObject(responseContent);
			return jsonObject.getString("server_name");
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}

	private JSONObject createJsonObject(InputStream in) throws JSONException {
		return new JSONObject(new JSONTokener(new InputStreamReader(in)));
	}
}

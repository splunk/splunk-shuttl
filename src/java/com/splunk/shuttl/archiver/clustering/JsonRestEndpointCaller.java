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
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.JSONTokener;

/**
 * Calls a REST endpoint, returning JSON.
 */
public class JsonRestEndpointCaller {

	private final HttpClient httpClient;

	public JsonRestEndpointCaller(HttpClient httpClient) {
		this.httpClient = httpClient;
	}

	/**
	 * @param httpRequest
	 *          which returns JSON.
	 */
	public JSONObject getJson(HttpUriRequest httpRequest) {
		HttpResponse response = getResponseFromRequest(httpRequest);
		InputStream content = getReponseContent(response);
		return extractJsonFromContent(content);
	}

	private HttpResponse getResponseFromRequest(HttpUriRequest request) {
		try {
			return httpClient.execute(request);
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

	private JSONObject extractJsonFromContent(InputStream content) {
		try {
			return new JSONObject(new JSONTokener(new InputStreamReader(content)));
		} catch (JSONException e) {
			throw new RuntimeException(e);
		}
	}
}

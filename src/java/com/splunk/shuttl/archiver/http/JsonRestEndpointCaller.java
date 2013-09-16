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
package com.splunk.shuttl.archiver.http;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

/**
 * Calls a REST endpoint, returning JSON.
 */
public class JsonRestEndpointCaller {

	private static final Logger logger = Logger
			.getLogger(JsonRestEndpointCaller.class);

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
		return extractJsonFromContent(toString(content));
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

	private String toString(InputStream content) {
		try {
			return IOUtils.toString(content);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private JSONObject extractJsonFromContent(String jsonString) {
		try {
			if (jsonString.trim().startsWith("{"))
				return new JSONObject(jsonString);
			else
				return createUnknownJsonContent(jsonString);
		} catch (JSONException e) {
			logger.error(did("Tried to create JSON object from string", e,
					"to create object", "json_string", jsonString));
			throw new RuntimeException(e);
		}
	}

	private JSONObject createUnknownJsonContent(String jsonString) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("unknown_response", jsonString);
		return new JSONObject(map);
	}

	public static JsonRestEndpointCaller create() {
		return new JsonRestEndpointCaller(new DefaultHttpClient());
	}
}

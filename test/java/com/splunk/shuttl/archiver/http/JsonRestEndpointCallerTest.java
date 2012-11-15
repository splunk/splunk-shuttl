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

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpUriRequest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.amazonaws.util.StringInputStream;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.archiver.http.JsonRestEndpointCaller;

@Test(groups = { "fast-unit" })
public class JsonRestEndpointCallerTest {

	private HttpClient httpClient;
	private JsonRestEndpointCaller restEndpointCaller;

	@BeforeMethod
	public void setUp() {
		httpClient = mock(HttpClient.class, RETURNS_DEEP_STUBS);
		restEndpointCaller = new JsonRestEndpointCaller(httpClient);
	}

	@SuppressWarnings("serial")
	public void getJson_givenRestEndpoint_jsonContainsContentFromHttpResponse()
			throws ClientProtocolException, IOException, JSONException {
		Map<String, String> mapWithKeyValue = new HashMap<String, String>() {
			{
				put("key", "value");
			}
		};
		JSONObject jsonObject = new JSONObject(mapWithKeyValue);

		HttpUriRequest httpRequest = mock(HttpUriRequest.class);
		HttpResponse response = mock(HttpResponse.class, RETURNS_DEEP_STUBS);
		when(httpClient.execute(httpRequest)).thenReturn(response);
		when(response.getEntity().getContent()).thenReturn(
				new StringInputStream(jsonObject.toString()));

		JSONObject actualJson = restEndpointCaller.getJson(httpRequest);
		assertEquals(actualJson.getString("key"), "value");
	}
}

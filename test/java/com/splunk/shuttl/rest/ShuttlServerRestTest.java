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
package com.splunk.shuttl.rest;

import static org.testng.Assert.*;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.ShuttlConstants;

@Test(groups = { "end-to-end" })
public class ShuttlServerRestTest {

	@Parameters(value = { "shuttl.host", "shuttl.port" })
	public void getDefaultHost(String shuttlHost, String shuttlPort)
			throws URISyntaxException, ClientProtocolException, IOException {
		URI defaultHostUri = getUriForServerEndpoint(shuttlHost, shuttlPort,
				ShuttlConstants.ENDPOINT_SHUTTL_HOST);

		HttpResponse response = getResponseFromShuttlServer(defaultHostUri);
		List<String> lines = getLinesFromResponse(response);
		assertEquals(1, lines.size());
		assertEquals("localhost", lines.get(0));
	}

	private URI getUriForServerEndpoint(String shuttlHost, String shuttlPort,
			String defaultPortEndpoint) throws URISyntaxException {
		String serverEndpoints = "/" + ShuttlConstants.ENDPOINT_CONTEXT
				+ ShuttlConstants.ENDPOINT_SERVER;
		return new URI("http", null, shuttlHost, Integer.parseInt(shuttlPort),
				serverEndpoints + defaultPortEndpoint, null, null);
	}

	private HttpResponse getResponseFromShuttlServer(URI requestUri)
			throws IOException, ClientProtocolException {
		HttpGet httpGet = new HttpGet(requestUri);
		HttpResponse response = new DefaultHttpClient().execute(httpGet);
		int status = response.getStatusLine().getStatusCode();
		assertEquals(200, status, "Unexpected status. response=" + response);
		return response;
	}

	private List<String> getLinesFromResponse(HttpResponse response)
			throws IOException {
		return IOUtils.readLines(response.getEntity().getContent());
	}

}

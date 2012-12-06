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
package com.splunk.shuttl.archiver.copy;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import java.io.IOException;
import java.net.URI;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.ShuttlServer;
import com.splunk.shuttl.testutil.TUtilsBucket;

@Test(groups = { "fast-unit" })
public class CallCopyBucketEndpointTest {

	private HttpClient httpClient;
	private ShuttlServer shuttlMBean;
	private CallCopyBucketEndpoint copyBucketEndpoint;

	@BeforeMethod
	public void setUp() {
		httpClient = mock(HttpClient.class);
		shuttlMBean = mock(ShuttlServer.class);

		copyBucketEndpoint = new CallCopyBucketEndpoint(httpClient, shuttlMBean);
	}

	public void _givenBucket_callsCopyBucketEndpointWithClientAndConfiguration()
			throws IOException {
		String host = "host";
		when(shuttlMBean.getHttpHost()).thenReturn(host);
		int port = 1234;
		when(shuttlMBean.getHttpPort()).thenReturn(port);
		LocalBucket bucket = TUtilsBucket.createBucket();

		copyBucketEndpoint.call(bucket);

		HttpPost capturedRequest = getHttpClientsExecutedRequest();
		URI capturedUri = capturedRequest.getURI();
		assertEquals(host, capturedUri.getHost());
		assertEquals(capturedUri.getPort(), port);
		// HttpParams caputredParams = capturedRequest.getParams();
		// String index = caputredParams.getParameter("index").toString();
		// assertEquals(index, bucket.getIndex());
		// String path = caputredParams.getParameter("path").toString();
		// assertEquals(path, bucket.getDirectory().getAbsolutePath());
	}

	private HttpPost getHttpClientsExecutedRequest() throws IOException,
			ClientProtocolException {
		ArgumentCaptor<HttpPost> requestCaptor = ArgumentCaptor
				.forClass(HttpPost.class);
		verify(httpClient).execute(requestCaptor.capture());
		return requestCaptor.getValue();
	}
}

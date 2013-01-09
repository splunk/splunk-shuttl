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
package com.splunk.shuttl.testutil;

import static org.testng.Assert.*;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

import com.splunk.shuttl.server.mbeans.util.EndpointUtils;

public class TUtilsEndToEnd {

	public static void callSlaveArchiveBucketEndpoint(String index,
			String bucketPath, String host, int shuttlPort) {
		HttpPost httpPost = EndpointUtils.createArchiveBucketPostRequest(host,
				shuttlPort, bucketPath, index);
		DefaultHttpClient httpClient = new DefaultHttpClient();
		HttpResponse response = executeHttp(httpPost, httpClient);

		assertEquals(204, response.getStatusLine().getStatusCode());
	}

	private static HttpResponse executeHttp(HttpPost httpPost,
			DefaultHttpClient httpClient) {
		try {
			return httpClient.execute(httpPost);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

}

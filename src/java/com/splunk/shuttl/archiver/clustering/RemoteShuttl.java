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

import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;

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
		HttpGet get = new HttpGet("http://" + hostname + ":" + shuttlPort + "/"
				+ ShuttlConstants.ENDPOINT_CONTEXT
				+ ShuttlConstants.ENDPOINT_SHUTTL_CONFIGURATION
				+ ShuttlConstants.ENDPOINT_CONFIG_SERVERNAME);
		return executeRequest(get);
	}

	private String executeRequest(HttpGet get) {
		List<String> readLines = getLinesFromRequest(get);
		return readLines.get(0).split(":")[1].replaceAll("}", "");
	}

	private List<String> getLinesFromRequest(HttpGet get) {
		try {
			HttpResponse response = defaultHttpClient.execute(get);
			return IOUtils.readLines(response.getEntity().getContent());
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

}

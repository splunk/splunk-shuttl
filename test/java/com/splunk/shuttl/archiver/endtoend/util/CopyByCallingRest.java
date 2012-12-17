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
package com.splunk.shuttl.archiver.endtoend.util;

import static org.testng.Assert.*;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;

import com.splunk.shuttl.archiver.endtoend.CopyWithoutDeletionEndToEndTest;
import com.splunk.shuttl.archiver.endtoend.CopyWithoutDeletionEndToEndTest.CopiesBucket;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Used for testing {@link CopyWithoutDeletionEndToEndTest}
 */
public class CopyByCallingRest implements CopiesBucket {

	private String shuttlHost;
	private String shuttlPort;

	public CopyByCallingRest(String shuttlHost, String shuttlPort) {
		this.shuttlHost = shuttlHost;
		this.shuttlPort = shuttlPort;
	}

	@Override
	public void copyBucket(LocalBucket bucket) {
		try {
			copyBucketViaRestCall(shuttlHost, shuttlPort, bucket);
		} catch (Exception e) {
			TUtilsTestNG.failForException("Got exception when copying bucket.", e);
		}
	}

	private void copyBucketViaRestCall(String shuttlHost, String shuttlPort,
			final LocalBucket bucket) throws IOException, ClientProtocolException {
		HttpPost copyBucketRequest = EndpointUtils.createCopyBucketPostRequest(
				shuttlHost, Integer.parseInt(shuttlPort), bucket);
		HttpResponse httpResponse = new DefaultHttpClient()
				.execute(copyBucketRequest);

		int statusCode = httpResponse.getStatusLine().getStatusCode();
		assertTrue(300 > statusCode,
				"Http endpoint status code not less than 300. Was: " + statusCode);
	}

}

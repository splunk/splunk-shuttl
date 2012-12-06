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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.archive.BucketFormat;
import com.splunk.shuttl.archiver.model.FileNotDirectoryException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.ShuttlServer;
import com.splunk.shuttl.server.mbeans.ShuttlServerMBean;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;

/**
 * Calls the REST endpoint for copying buckets, after have locked the bucket.
 */
public class CallCopyBucketEndpoint {

	private static final Logger logger = Logger
			.getLogger(CallCopyBucketEndpoint.class);

	private HttpClient httpClient;
	private ShuttlServerMBean shuttlMBean;

	public CallCopyBucketEndpoint(HttpClient httpClient,
			ShuttlServerMBean shuttlMBean) {
		this.httpClient = httpClient;
		this.shuttlMBean = shuttlMBean;
	}

	public void call(LocalBucket bucket) {
		String host = shuttlMBean.getHttpHost();
		int port = shuttlMBean.getHttpPort();
		HttpPost copyBucketRequest = EndpointUtils.createCopyBucketPostRequest(
				host, port, bucket);
		try {
			httpClient.execute(copyBucketRequest);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	public static void main(String[] args) throws FileNotFoundException,
			FileNotDirectoryException {
		File bucketDir = new File(args[0]);
		LocalBucket bucket = new LocalBucket(bucketDir,
				IndexScanner.getIndexNameByBucketPath(bucketDir),
				BucketFormat.SPLUNK_BUCKET);

		ShuttlServerMBean serverMBean = ShuttlServer
				.getRegisteredServerMBean(logger);

		CallCopyBucketEndpoint callCopyBucketEndpoint = new CallCopyBucketEndpoint(
				new DefaultHttpClient(), serverMBean);

		callCopyBucketEndpoint.call(bucket);

		Logger.getLogger(CallCopyBucketEndpoint.class).info(
				"Main called with args: " + Arrays.toString(args));
	}

}

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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.server.mbeans.ShuttlServerMBean;
import com.splunk.shuttl.server.mbeans.util.EndpointUtils;

/**
 * Calls the REST endpoint for copying buckets, after have locked the bucket.
 */
public class CallCopyBucketEndpoint {

	private static final Logger logger = Logger
			.getLogger(CallCopyBucketEndpoint.class);

	private final HttpClient httpClient;
	private final ShuttlServerMBean shuttlMBean;
	private final ResponseHandler responseHandler;

	public CallCopyBucketEndpoint(HttpClient httpClient,
			ShuttlServerMBean shuttlMBean, ResponseHandler responseHandler) {
		this.httpClient = httpClient;
		this.shuttlMBean = shuttlMBean;
		this.responseHandler = responseHandler;
	}

	public void call(LocalBucket bucket) {
		String host = shuttlMBean.getHttpHost();
		int port = shuttlMBean.getHttpPort();
		HttpPost copyBucketRequest = EndpointUtils.createCopyBucketPostRequest(
				host, port, bucket);
		try {
			HttpResponse response = httpClient.execute(copyBucketRequest);
			responseHandler.throwIfResponseWasNot2xx(bucket, response);
		} catch (IOException e) {
			logger.error(did("Called copy bucket endpoint", e,
					"to execute without failure", "bucket", bucket));
			throw new NonSuccessfulBucketCopy(e);
		}
	}

	public static class ResponseHandler {

		public void throwIfResponseWasNot2xx(LocalBucket bucket,
				HttpResponse response) {
			int statusCode = response.getStatusLine().getStatusCode();
			if (!isHttpOkStatus(statusCode)) {
				logger
						.warn(warn("Http status was not 2xx", "was: " + statusCode,
								"will throw exception", "bucket", bucket, "http_status",
								statusCode));
				throw new NonSuccessfulBucketCopy(response.getStatusLine());
			}
		}

		private boolean isHttpOkStatus(int statusCode) {
			return statusCode >= 200 && statusCode < 300;
		}

	}

	public static class NonSuccessfulBucketCopy extends RuntimeException {

		public NonSuccessfulBucketCopy(IOException e) {
			super(e);
		}

		public NonSuccessfulBucketCopy(StatusLine statusLine) {
			super("Got non OK http status: " + statusLine.getStatusCode());
		}

		private static final long serialVersionUID = 1L;

	}

	public static CallCopyBucketEndpoint create(ShuttlServerMBean serverMBean) {
		return new CallCopyBucketEndpoint(new DefaultHttpClient(), serverMBean,
				new ResponseHandler());
	}

}

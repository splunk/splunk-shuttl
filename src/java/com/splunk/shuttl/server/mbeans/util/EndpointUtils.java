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
package com.splunk.shuttl.server.mbeans.util;

import static com.splunk.shuttl.ShuttlConstants.*;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;

import com.splunk.shuttl.archiver.model.LocalBucket;

/**
 * Utils for the REST endpoints
 */
public class EndpointUtils {

	public static HttpPost createCopyBucketPostRequest(String shuttlHost,
			int shuttlPort, LocalBucket bucket) {
		return createArchiverPostRequest(shuttlHost, shuttlPort, bucket
				.getDirectory().getAbsolutePath(), bucket.getIndex(),
				ENDPOINT_BUCKET_COPY);
	}

	public static HttpPost createArchiveBucketPostRequest(String shuttlHost,
			int shuttlPort, String bucketPath, String index) {
		return createArchiverPostRequest(shuttlHost, shuttlPort, bucketPath, index,
				ENDPOINT_BUCKET_ARCHIVE);
	}

	private static HttpPost createArchiverPostRequest(String shuttlHost,
			int shuttlPort, String bucketPath, String index, String endpoint) {
		URI copyBucketEndpoint = URI.create("http://" + shuttlHost + ":"
				+ shuttlPort + "/" + ENDPOINT_CONTEXT + ENDPOINT_ARCHIVER + endpoint);
		HttpPost postRequest = createHttpPost(copyBucketEndpoint, "path",
				bucketPath, "index", index);
		return postRequest;
	}

	/**
	 * @param endpoint
	 * @param POST
	 *          key values
	 */
	public static HttpPost createHttpPost(URI endpoint, Object... kvs) {
		HttpPost httpPost = new HttpPost(endpoint);

		List<BasicNameValuePair> postParams = new ArrayList<BasicNameValuePair>();
		for (int i = 0; i < kvs.length; i += 2)
			postParams.add(createNameValuePair(kvs[i], kvs[i + 1]));

		setParamsToPostRequest(httpPost, postParams);
		return httpPost;
	}

	private static BasicNameValuePair createNameValuePair(Object name,
			Object value) {
		return new BasicNameValuePair(name.toString(), value.toString());
	}

	/**
	 * Set data params to a post request.
	 */
	public static void setParamsToPostRequest(HttpPost httpPost,
			List<BasicNameValuePair> postParams) {
		try {
			httpPost.setEntity(new UrlEncodedFormEntity(postParams));
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}

}

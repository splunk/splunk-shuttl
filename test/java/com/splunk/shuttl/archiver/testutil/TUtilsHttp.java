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
package com.splunk.shuttl.archiver.testutil;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;

import com.splunk.shuttl.testutil.TUtilsTestNG;

public class TUtilsHttp {

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

		TUtilsHttp.setParamsToPostRequest(httpPost, postParams);
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
			TUtilsTestNG
					.failForException(
							"Could not create url encoded form entity with params: "
									+ postParams, e);
		}
	}
}

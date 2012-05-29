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
package com.splunk.shuttl.archiver.archive;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.splunk.shuttl.ShuttlConstants;
import com.splunk.shuttl.archiver.archive.recovery.BucketLocker.SharedLockBucketHandler;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.server.mbeans.rest.BucketArchiverRest;

/**
 * Handling all the calls and returns to and from {@link BucketArchiverRest}
 */
public class ArchiveRestHandler implements SharedLockBucketHandler {

	private final HttpClient httpClient;
	private final Logger logger;

	public ArchiveRestHandler(HttpClient httpClient) {
		this(httpClient, Logger.getLogger(ArchiveRestHandler.class));
	}

	public ArchiveRestHandler(HttpClient httpClient, Logger logger) {
		this.httpClient = httpClient;
		this.logger = logger;
	}

	public void callRestToArchiveBucket(Bucket bucket) {
		HttpResponse response = null;
		try {
			HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
			response = executeArchiveBucketRequest(bucket, archiveBucketRequest);
		} catch (HttpResponseException e) {
			logHttpResponseException(bucket, e);
		} catch (IOException e) {
			logIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
		} finally {
			if (response != null)
				consumeResponseHandlingErrors(response);
		}
	}

	private static HttpUriRequest createBucketArchiveRequest(Bucket bucket)
			throws UnsupportedEncodingException {
		// CONFIG configure the host and port with a general solution.
		String requestString = "http://localhost:9090/"
				+ ShuttlConstants.ENDPOINT_CONTEXT + ShuttlConstants.ENDPOINT_ARCHIVER
				+ ShuttlConstants.ENDPOINT_BUCKET_ARCHIVER;

		HttpPost request = new HttpPost(requestString);

		ArrayList<NameValuePair> params = new ArrayList<NameValuePair>();
		params.add(new BasicNameValuePair("path", bucket.getDirectory()
				.getAbsolutePath()));
		params.add(new BasicNameValuePair("index", bucket.getIndex()));

		request.setEntity(new UrlEncodedFormEntity(params));
		return request;
	}

	private HttpResponse executeArchiveBucketRequest(Bucket bucket,
			HttpUriRequest archiveBucketRequest) throws IOException,
			ClientProtocolException, HttpResponseException {
		logger.debug(will("Send an archive bucket request", "request_uri",
				archiveBucketRequest.getURI()));
		HttpResponse response = httpClient.execute(archiveBucketRequest);
		handleResponseFromDoingArchiveBucketRequest(response, bucket);
		return response;
	}

	private void handleResponseFromDoingArchiveBucketRequest(
			HttpResponse response, Bucket bucket) throws HttpResponseException {
		switch (response.getStatusLine().getStatusCode()) {
		case HttpStatus.SC_OK:
		case HttpStatus.SC_NO_CONTENT:
			logSuccess(response, bucket, response.getStatusLine().getStatusCode());
			break;
		default:
			throw new HttpResponseException(response.getStatusLine().getStatusCode(),
					"Unexpected response when archiving bucket.");
		}
	}

	private void logSuccess(HttpResponse response, Bucket bucket, int statusCode) {
		String entity = getEntityFromResponse(response);
		logger.debug(done("Got http response from archiveBucketRequest",
				"status_code", statusCode, "bucket_name", bucket.getName(), "entity",
				entity));
	}

	/**
	 * @return entity from the response as a string.
	 */
	private String getEntityFromResponse(HttpResponse response) {
		try {
			HttpEntity entity = response.getEntity();
			return entity != null ? EntityUtils.toString(entity) : "";
		} catch (IOException e) {
			// Ignore.
			return "";
		}
	}

	private void logHttpResponseException(Bucket bucket, HttpResponseException e) {
		logger.error(did("Sent an archive bucket reuqest",
				"Got non ok http_status", "expected HttpStatus.SC_OK or SC_NO_CONTENT",
				"http_status", e.getStatusCode(), "bucket_name", bucket.getName()));
	}

	private void logIOExceptionGenereratedByDoingArchiveBucketRequest(
			IOException e, Bucket bucket) {
		logger.error(did("Sent archive bucket request", "got IOException",
				"request to succeed", "exception", e, "bucket_name", bucket.getName(),
				"cause", e.getCause()));
	}

	private void consumeResponseHandlingErrors(HttpResponse response) {
		try {
			EntityUtils.consume(response.getEntity());
		} catch (IOException e) {
			logger.error(did(
					"Tried to consume http response of archive bucket request", e,
					"no exception", "response", response));
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * com.splunk.shuttl.archiver.archive.recovery.BucketLocker.LockedBucketHandler
	 * #handleLockedBucket(com.splunk.shuttl.archiver.model.Bucket)
	 */
	@Override
	public void handleSharedLockedBucket(Bucket bucket) {
		callRestToArchiveBucket(bucket);
	}

}

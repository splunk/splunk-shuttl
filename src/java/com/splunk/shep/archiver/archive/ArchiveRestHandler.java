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
package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;

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

import com.splunk.shep.ShepConstants;
import com.splunk.shep.archiver.archive.recovery.BucketLocker.SharedLockBucketHandler;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.server.mbeans.rest.BucketArchiverRest;

/**
 * Handling all the calls and returns to and from {@link BucketArchiverRest}
 */
public class ArchiveRestHandler implements SharedLockBucketHandler {

    private final static Logger logger = Logger
	    .getLogger(ArchiveRestHandler.class);
    private final HttpClient httpClient;

    public ArchiveRestHandler(HttpClient httpClient) {
	this.httpClient = httpClient;
    }

    public void callRestToArchiveBucket(Bucket bucket) {
	HttpResponse response = null;

	try {
	    HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);

	    if (logger.isDebugEnabled()) {
		logger.debug(will("Send an archive bucket request",
			"request_uri", archiveBucketRequest.getURI()));
	    }

	    response = httpClient.execute(archiveBucketRequest);

	    if (response != null) {
		handleResponseFromDoingArchiveBucketRequest(response, bucket);
	    } else {
		// LOG: warning! Response was null. This happens in our tests
		// when we mock the httpClient. Should never happen otherwise.
		// Should it?
		logger.warn(did("Sent an archive bucket request",
			"Got a null response", "A non-null response"));
	    }
	} catch (ClientProtocolException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	} catch (IOException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	} finally {
	    consumeResponseHandlingErrors(response);
	}
    }

    private void consumeResponseHandlingErrors(HttpResponse response) {
	if (response != null) {
	    try {
		EntityUtils.consume(response.getEntity());
	    } catch (IOException e) {
		logger.error(did(
			"Tried to consume http response of archive bucket request",
			e, "no exception", "response", response));
	    }
	}
    }

    private void handleResponseFromDoingArchiveBucketRequest(
	    HttpResponse response, Bucket bucket) throws HttpResponseException {
	int statusCode = response.getStatusLine().getStatusCode();

	String entity = null;
	try {
	    entity = EntityUtils.toString(response.getEntity());
	} catch (Exception e) {
	    // ignore Exceptions - we just want the entity for logging anyway
	}

	// TODO handle the different status codes
	switch (statusCode) {
	case HttpStatus.SC_OK:
	case HttpStatus.SC_NO_CONTENT:
	    if (logger.isDebugEnabled()) {
		logger.debug(done(
			"Got http response from archiveBucketRequest",
			"status_code", statusCode, "bucket_name",
			bucket.getName(), "entity", entity));
	    }
	    break;
	default:
	    logger.error(did("Sent an archive bucket reuqest",
		    "Got non ok http_status",
		    "expected HttpStatus.SC_OK or SC_NO_CONTENT",
		    "http_status", statusCode, "bucket_name", bucket.getName(),
		    "entity", entity));
	    throw new HttpResponseException(statusCode,
		    "Unexpected response when archiving bucket.");
	}
    }

    private void handleIOExceptionGenereratedByDoingArchiveBucketRequest(
	    IOException e, Bucket bucket) {

	// TODO this method should handle the errors in case the bucket transfer
	// fails. In this state there is no way of telling if the bucket was
	// actually transfered or not.

	logger.error(did("Sent archive bucket request", "got IOException",
		"request to succeed", "exception", e, "bucket_name",
		bucket.getName(), "cause", e.getCause()));
	throw new RuntimeException(e);
    }

    private static HttpUriRequest createBucketArchiveRequest(Bucket bucket)
	    throws UnsupportedEncodingException {
	// CONFIG configure the host and port with a general solution.
	String requestString = "http://localhost:9090/"
		+ ShepConstants.ENDPOINT_CONTEXT
		+ ShepConstants.ENDPOINT_ARCHIVER
		+ ShepConstants.ENDPOINT_BUCKET_ARCHIVER;

	HttpPost request = new HttpPost(requestString);

	ArrayList<NameValuePair> params = new ArrayList<NameValuePair>();
	params.add(new BasicNameValuePair("path", bucket.getDirectory()
		.getAbsolutePath()));
	params.add(new BasicNameValuePair("index", bucket.getIndex()));

	request.setEntity(new UrlEncodedFormEntity(params));
	return request;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.splunk.shep.archiver.archive.recovery.BucketLocker.LockedBucketHandler
     * #handleLockedBucket(com.splunk.shep.archiver.model.Bucket)
     */
    @Override
    public void handleSharedLockedBucket(Bucket bucket) {
	callRestToArchiveBucket(bucket);
    }

}

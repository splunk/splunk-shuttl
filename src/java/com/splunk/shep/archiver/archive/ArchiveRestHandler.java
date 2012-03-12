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

import static com.splunk.shep.archiver.ArchiverLogger.did;
import static com.splunk.shep.archiver.ArchiverLogger.done;
import static com.splunk.shep.archiver.ArchiverLogger.will;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;

import com.splunk.shep.archiver.archive.recovery.FailedBucketTransfers;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.server.mbeans.rest.BucketArchiverRest;

/**
 * Handling all the calls and returns to and from {@link BucketArchiverRest}
 */
public class ArchiveRestHandler {

    HttpClient httpClient;
    private final FailedBucketTransfers failedBucketTransfers;

    /**
     * TODO:
     */
    public ArchiveRestHandler(HttpClient httpClient,
	    FailedBucketTransfers failedBucketTransfers) {
	this.httpClient = httpClient;
	this.failedBucketTransfers = failedBucketTransfers;
    }

    public void callRestToArchiveBucket(Bucket bucket) {
	HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
	try {
	    will("Send an archive bucket request", "request_uri",
		    archiveBucketRequest.getURI());
	    HttpResponse response = httpClient.execute(archiveBucketRequest); // LOG
	    if (response != null) {
		handleResponseCodeFromDoingArchiveBucketRequest(response
			.getStatusLine().getStatusCode(), bucket);
	    } else {
		// It is test code.
		// TODO: Get rid of this bad design.
	    }
	} catch (ClientProtocolException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	} catch (IOException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e, bucket);
	}
    }

    private void handleResponseCodeFromDoingArchiveBucketRequest(
	    int statusCode, Bucket bucket) {
	// TODO handle the different status codes
	switch (statusCode) {
	case HttpStatus.SC_OK:
	case HttpStatus.SC_NO_CONTENT:
	    done("Got http response from archiveBucketRequest", "status_code",
		    statusCode);
	    break;
	default:
	    will("Move bucket to failed buckets location because of failed HttpStatus",
		    "bucket", bucket, "status_code", statusCode);
	    failedBucketTransfers.moveFailedBucket(bucket);
	}
    }

    private void handleIOExceptionGenereratedByDoingArchiveBucketRequest(
	    IOException e, Bucket bucket) {
	did("Archive bucket request", "got IOException", "request to succeed",
		"exception", e);
	// TODO this method should handle the errors in case the bucket transfer
	// fails. In this state there is no way of telling if the bucket was
	// actually transfered or not.
	// REVIEW: Moving bucket to failed bucket location, just in case. And
	// the next time this bucket gets transfered, we have to check whether
	// it should be archived or not.
	will("Move bucket to failed bucket location because of exception",
		"bucket", bucket, "exception", e);
	failedBucketTransfers.moveFailedBucket(bucket);
    }

    private HttpUriRequest createBucketArchiveRequest(Bucket bucket) {
	// CONFIG configure the host, port, request URL with a general
	// solution.
	String requestString = "http://localhost:9090/shep/rest/archiver/bucket/archive?path="
		+ bucket.getDirectory().getAbsolutePath()
		+ "&index="
		+ bucket.getIndex();
	HttpGet request = new HttpGet(requestString);
	return request;
    }

}

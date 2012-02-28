package com.splunk.shep.archiver.archive;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.DefaultHttpClient;

import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

public class BucketFreezer {

    // CONFIG get this value from the config.
    public static final String DEFAULT_SAFE_LOCATION = System
	    .getProperty("user.home") + "/" + BucketFreezer.class.getName();

    private final String safeLocationForBuckets;
    private HttpClient httpClient;

    protected BucketFreezer(String safeLocationForBuckets, HttpClient httpClient) {
	this.safeLocationForBuckets = safeLocationForBuckets;
	this.httpClient = httpClient;
    }

    public int freezeBucket(String path) {
	try {
	    moveAndArchiveBucket(path);
	    return 0;
	} catch (IOException e) {
	    System.err.println(e.getMessage());
	    return 3;
	}
    }

    private void moveAndArchiveBucket(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = Bucket.createWithAbsolutePath(path);
	Bucket safeBucket = bucket.moveBucketToDir(createSafeLocation());
	doRestCall(safeBucket);
    }

    private File createSafeLocation() {
	File safeLocation = new File(safeLocationForBuckets);
	safeLocation.mkdirs();
	return safeLocation;
    }

    private void doRestCall(Bucket bucket) {
	HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
	try {
	    HttpResponse response = httpClient.execute(archiveBucketRequest); // LOG
	    hadnleResponseCodeFromDoingArchiveBucketRequest(response
		    .getStatusLine().getStatusCode());
	} catch (ClientProtocolException e) {
	    hadleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
	} catch (IOException e) {
	    hadleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
	}
    }

    private void hadnleResponseCodeFromDoingArchiveBucketRequest(int statusCode) {
	// TODO handle the different status codes
	switch (statusCode) {
	case HttpStatus.SC_OK:
	    // LOG
	    break;
	default:
	    // LOG
	    throw new RuntimeException("Got the response code " + statusCode
		    + " from making the archiveBucketRequest.");
	}
    }

    private void hadleIOExceptionGenereratedByDoingArchiveBucketRequest(
	    IOException e) {
	// LOG
	// TODO this method should handle the errors in case the bucket transfer
	// fails. In this state there is no way of telling if the bucket was
	// actually trasfered or not.
	throw new RuntimeException(e);
    }

    private HttpUriRequest createBucketArchiveRequest(Bucket bucket) {
	// CONFIG configure the host, port, request URL with a general
	// solution.
	HttpGet request = new HttpGet(
		"http://localhost:9090/shep/rest/archiver/bucket/archive?path=/path/to/bucket");
	return request;
    }

    public static BucketFreezer createWithDeafultSafeLocationAndHTTPClient() {
	return new BucketFreezer(DEFAULT_SAFE_LOCATION, new DefaultHttpClient());
    }

    public static void main(String[] args) {
	if (args.length == 0)
	    System.exit(1);
	if (args.length >= 2)
	    System.exit(2);
	BucketFreezer archiveBucket = BucketFreezer
		.createWithDeafultSafeLocationAndHTTPClient();
	int exitStatus = archiveBucket.freezeBucket(args[0]);
	System.exit(exitStatus);
    }

}

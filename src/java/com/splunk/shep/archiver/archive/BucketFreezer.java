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
    /* package-private */HttpClient httpClient;

    protected BucketFreezer(String safeLocationForBuckets, HttpClient httpClient) {
	this.safeLocationForBuckets = safeLocationForBuckets;
	this.httpClient = httpClient;
    }

    public int freezeBucket(String path) {
	try {
	    moveAndArchiveBucket(path);
	    return 0;
	} catch (FileNotDirectoryException e) {
	    return 3;
	} catch (FileNotFoundException e) {
	    return 4;
	}
    }

    private void moveAndArchiveBucket(String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = Bucket.createWithAbsolutePath(path);
	Bucket safeBucket = bucket.moveBucketToDir(getSafeLocation());
	doRestCall(safeBucket);
    }

    private File getSafeLocation() {
	File safeLocation = new File(safeLocationForBuckets);
	safeLocation.mkdirs();
	return safeLocation;
    }

    private void doRestCall(Bucket bucket) {
	HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
	try {
	    HttpResponse response = httpClient.execute(archiveBucketRequest); // LOG
	    handleResponseCodeFromDoingArchiveBucketRequest(response
		    .getStatusLine().getStatusCode());
	} catch (ClientProtocolException e) {
	    hadleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
	} catch (IOException e) {
	    hadleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
	}
    }

    private void handleResponseCodeFromDoingArchiveBucketRequest(int statusCode) {
	// TODO handle the different status codes
	switch (statusCode) {
	case HttpStatus.SC_OK:
	    // LOG
	    break;
	case HttpStatus.SC_NO_CONTENT:
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
	throw new RuntimeException("Got IOException" + e);
    }

    private HttpUriRequest createBucketArchiveRequest(Bucket bucket) {
	// CONFIG configure the host, port, request URL with a general
	// solution.
	String requestString = "http://localhost:9090/shep/rest/archiver/bucket/archive?path="
		+ bucket.getDirectory().getAbsolutePath();
	HttpGet request = new HttpGet(requestString);
	return request;
    }

    public static BucketFreezer createWithDeafultSafeLocationAndHTTPClient() {
	return new BucketFreezer(DEFAULT_SAFE_LOCATION, new DefaultHttpClient());
    }

    /* package-private */static void runMainWithDepentencies(Runtime runtime,
	    BucketFreezer bucketFreezer, String... args) {
	if (args.length == 0) {
	    runtime.exit(1);
	} else if (args.length >= 2) {
	    runtime.exit(2);
	} else {
	    runtime.exit(bucketFreezer.freezeBucket(args[0]));
	}
    }

    public static void main(String... args) {
	runMainWithDepentencies(Runtime.getRuntime(),
		BucketFreezer.createWithDeafultSafeLocationAndHTTPClient(),
		args);
    }

}

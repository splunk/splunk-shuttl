package com.splunk.shep.archiver.archive;

import static com.splunk.shep.archiver.ArchiverLogger.*;

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

import com.splunk.shep.archiver.archive.recovery.FailedBucketLock;
import com.splunk.shep.archiver.archive.recovery.FailedBucketRecoveryHandler;
import com.splunk.shep.archiver.archive.recovery.FailedBucketRestorer;
import com.splunk.shep.archiver.archive.recovery.FailedBucketTransfers;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

public class BucketFreezer {

    // CONFIG get this value from the config.
    public static final String DEFAULT_SAFE_LOCATION = System
	    .getProperty("user.home")
	    + "/"
	    + BucketFreezer.class.getName()
	    + "-safe-buckets";

    // CONFIG
    public static final String DEFAULT_FAIL_LOCATION = System
	    .getProperty("user.home")
	    + "/"
	    + BucketFreezer.class.getName()
	    + "-failed-buckets";

    private final String safeLocationForBuckets;
    /* package-private */HttpClient httpClient;
    private final FailedBucketTransfers failedBucketTransfers;
    /* package-private */FailedBucketRestorer failedBucketRestorer;

    protected BucketFreezer(String safeLocationForBuckets,
	    HttpClient httpClient, FailedBucketTransfers failedBucketTransfers,
	    FailedBucketRestorer failedBucketRestorer) {
	this.safeLocationForBuckets = safeLocationForBuckets;
	this.httpClient = httpClient;
	this.failedBucketTransfers = failedBucketTransfers;
	this.failedBucketRestorer = failedBucketRestorer;
    }

    public static final int EXIT_OK = 0;
    public static final int EXIT_INCORRECT_ARGUMENTS = -1;
    public static final int EXIT_FILE_NOT_A_DIRECTORY = -2;
    public static final int EXIT_FILE_NOT_FOUND = -3;

    /**
     * Freezez the bucket on the speicifed path and belonging to speicifed
     * index.
     * 
     * @param indexName
     *            The name of the index that this bucket belongs to
     * @param path
     *            The path of the bucket on the local file stystem
     * @return An exit code depending on the outcome.
     */
    public int freezeBucket(String indexName, String path) {
	try {
	    moveAndArchiveBucket(indexName, path);
	    return EXIT_OK;
	} catch (FileNotDirectoryException e) {
	    return EXIT_FILE_NOT_A_DIRECTORY;
	} catch (FileNotFoundException e) {
	    return EXIT_FILE_NOT_FOUND;
	}
    }

    private void moveAndArchiveBucket(String indexName, String path)
	    throws FileNotFoundException, FileNotDirectoryException {
	Bucket bucket = new Bucket(indexName, path);
	bucket = bucket.moveBucketToDir(getSafeLocationForBucket(bucket));
	doRestCall(bucket);
	failedBucketRestorer
		.recoverFailedBuckets(new CallRestToReArchiveFailedBuckets());
    }

    private File getSafeLocationForBucket(Bucket bucket) {
	File safeBucketLocation = new File(getSafeLocationRoot(),
		bucket.getIndex());
	safeBucketLocation.mkdirs();
	return safeBucketLocation;
    }

    private File getSafeLocationRoot() {
	return createDirectory(safeLocationForBuckets);
    }

    private void doRestCall(Bucket bucket) {
	HttpUriRequest archiveBucketRequest = createBucketArchiveRequest(bucket);
	try {
	    will("Send an archive bucket request", "request",
		    archiveBucketRequest);
	    HttpResponse response = httpClient.execute(archiveBucketRequest); // LOG
	    if (response != null) {
		handleResponseCodeFromDoingArchiveBucketRequest(response
			.getStatusLine().getStatusCode(), bucket);
	    } else {
		// It is test code.
		// TODO: Get rid of this bad design.
	    }
	} catch (ClientProtocolException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
	} catch (IOException e) {
	    handleIOExceptionGenereratedByDoingArchiveBucketRequest(e);
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

    private File createDirectory(String path) {
	File file = new File(path);
	file.mkdirs();
	return file;
    }

    private void handleIOExceptionGenereratedByDoingArchiveBucketRequest(
	    IOException e) {
	did("Archive bucket request", "got IOException", "request to succeed",
		"exception", e);
	// TODO this method should handle the errors in case the bucket transfer
	// fails. In this state there is no way of telling if the bucket was
	// actually transfered or not.
	throw new RuntimeException("Got IOException" + e);
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

    public static BucketFreezer createWithDeafultSafeLocationAndHTTPClient() {
	FailedBucketTransfers failedBucketTransfers = new FailedBucketTransfers(
		DEFAULT_FAIL_LOCATION);
	FailedBucketRestorer failedBucketRestorer = new FailedBucketRestorer(
		failedBucketTransfers, new FailedBucketLock());
	return new BucketFreezer(DEFAULT_SAFE_LOCATION,
		new DefaultHttpClient(), failedBucketTransfers,
		failedBucketRestorer);
    }

    private class CallRestToReArchiveFailedBuckets implements
	    FailedBucketRecoveryHandler {

	@Override
	public void recoverFailedBucket(Bucket failedBucket) {
	    doRestCall(failedBucket);
	}
    }

    /**
     * This method is used by the real main and only exists so that it can be
     * tested using test doubles.
     */
    /* package-private */static void runMainWithDepentencies(Runtime runtime,
	    BucketFreezer bucketFreezer, String... args) {
	if (args.length != 2) {
	    runtime.exit(EXIT_INCORRECT_ARGUMENTS);
	} else {
	    runtime.exit(bucketFreezer.freezeBucket(args[0], args[1]));
	}
    }

    public static void main(String... args) {
	runMainWithDepentencies(Runtime.getRuntime(),
		BucketFreezer.createWithDeafultSafeLocationAndHTTPClient(),
		args);
    }

}

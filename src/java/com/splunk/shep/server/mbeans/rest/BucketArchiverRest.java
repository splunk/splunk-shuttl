package com.splunk.shep.server.mbeans.rest;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;

/**
 * REST endpoint for archiving a bucket.
 */
@Path("/archiver")
public class BucketArchiverRest {

    /**
     * Example on how to archive a bucket with this endpoint:
     * /archiver/bucket/archive?path=/local/Path/To/Bucket
     * 
     * @param path
     *            to the bucket to be archived.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/bucket/archive")
    public void archiveBucket(@QueryParam("path") String path,
	    @QueryParam("index") String indexName) {
	archiveBucketOnAnotherThread(indexName, path);
    }

    private void archiveBucketOnAnotherThread(String indexName, String path) {
	Runnable r = new BucketArchiverRunner(indexName, path);
	new Thread(r).run();
    }

    private static class BucketArchiverRunner implements Runnable {

	private final String pathToBucket;
	private final String indexName;

	public BucketArchiverRunner(String indexName, String pathToBucket) {
	    this.pathToBucket = pathToBucket;
	    this.indexName = indexName;
	}

	@Override
	public void run() {
	    Bucket bucket = createBucketWithErrorHandling();
	    BucketArchiver bucketArchiver = BucketArchiverFactory
		    .createDefaultArchiver();
	    bucketArchiver.archiveBucket(bucket);
	    deleteBucketWithErrorHandling(bucket);
	}

	private Bucket createBucketWithErrorHandling() {
	    Bucket bucket;
	    try {
		bucket = new Bucket(indexName, pathToBucket);
	    } catch (FileNotFoundException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	    } catch (FileNotDirectoryException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	    }
	    return bucket;
	}

	private void deleteBucketWithErrorHandling(Bucket bucket) {
	    try {
		bucket.deleteBucket();
	    } catch (IOException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	    }
	}
    }
}

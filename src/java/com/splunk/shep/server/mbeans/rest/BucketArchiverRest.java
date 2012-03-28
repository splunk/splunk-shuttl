package com.splunk.shep.server.mbeans.rest;

import static com.splunk.shep.ShepConstants.ENDPOINT_ARCHIVER;
import static com.splunk.shep.ShepConstants.ENDPOINT_BUCKET_ARCHIVER;
import static com.splunk.shep.ShepConstants.ENDPOINT_CONTEXT;

import java.io.FileNotFoundException;
import java.io.IOException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;
import com.splunk.shep.metrics.ShepMetricsHelper;

/**
 * REST endpoint for archiving a bucket.
 */
@Path(ENDPOINT_ARCHIVER)
public class BucketArchiverRest {
    private org.apache.log4j.Logger logger = Logger.getLogger(getClass());

    /**
     * Example on how to archive a bucket with this endpoint:
     * /archiver/bucket/archive?path=/local/Path/To/Bucket
     * 
     * @param path
     *            to the bucket to be archived.
     */
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_BUCKET_ARCHIVER)
    public void archiveBucket(@QueryParam("path") String path,
	    @QueryParam("index") String indexName) {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_ARCHIVER, ENDPOINT_BUCKET_ARCHIVER);
	ShepMetricsHelper.update(logger, logMessage);

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

package com.splunk.shep.server.mbeans.rest;

import java.io.FileNotFoundException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.splunk.shep.archiver.archive.BucketArchiver;
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
    public void archiveBucket(@QueryParam("path") String path) {
	archiveBucketOnAnotherThread(path);
    }

    private void archiveBucketOnAnotherThread(String path) {
	Runnable r = new BucketArchiverRunner(path);
	new Thread(r).run();
    }

    private static class BucketArchiverRunner implements Runnable {

	private final String pathToBucket;

	public BucketArchiverRunner(String pathToBucket) {
	    this.pathToBucket = pathToBucket;
	}

	@Override
	public void run() {
	    try {
		BucketArchiver.create().archiveBucket(
			Bucket.createWithAbsolutePath(pathToBucket));
	    } catch (FileNotFoundException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	    } catch (FileNotDirectoryException e) {
		e.printStackTrace();
		throw new RuntimeException(e);
	    }
	}
    }
}

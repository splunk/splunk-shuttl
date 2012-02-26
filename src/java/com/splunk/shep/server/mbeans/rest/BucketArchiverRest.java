package com.splunk.shep.server.mbeans.rest;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

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
	// Bucket bucket = new Bucket();
	// BucketArchiver.create().archiveBucket(bucket);
	throw new UnsupportedOperationException();
    }
}

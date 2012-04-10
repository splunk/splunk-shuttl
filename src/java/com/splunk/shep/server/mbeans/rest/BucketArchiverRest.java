package com.splunk.shep.server.mbeans.rest;

import static com.splunk.shep.ShepConstants.*;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shep.archiver.archive.ArchiveConfiguration;
import com.splunk.shep.archiver.archive.BucketArchiver;
import com.splunk.shep.archiver.archive.BucketArchiverFactory;
import com.splunk.shep.archiver.archive.BucketArchiverRunner;
import com.splunk.shep.archiver.archive.PathResolver;
import com.splunk.shep.archiver.archive.recovery.BucketLock;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shep.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shep.archiver.listers.ArchiveBucketsLister;
import com.splunk.shep.archiver.listers.ArchivedIndexesLister;
import com.splunk.shep.archiver.model.Bucket;
import com.splunk.shep.archiver.model.FileNotDirectoryException;
import com.splunk.shep.archiver.thaw.BucketFormatChooser;
import com.splunk.shep.archiver.thaw.BucketFormatResolver;
import com.splunk.shep.archiver.thaw.BucketThawer;
import com.splunk.shep.archiver.thaw.BucketThawerFactory;
import com.splunk.shep.archiver.thaw.StringDateConverter;
import com.splunk.shep.metrics.ShepMetricsHelper;
import com.splunk.shep.server.model.BucketBean;

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
	logMetricsAtEndpoint(ENDPOINT_BUCKET_ARCHIVER);

	archiveBucketOnAnotherThread(indexName, path);
    }

    private void archiveBucketOnAnotherThread(String indexName, String path) {
	Runnable r = createBucketArchiverRunner(indexName, path);
	new Thread(r).run();
    }

    private Runnable createBucketArchiverRunner(String indexName, String path) {
	BucketArchiver bucketArchiver = BucketArchiverFactory
		.createConfiguredArchiver();
	Bucket bucket = createBucketWithErrorHandling(indexName, path);
	BucketLock bucketLock = new BucketLock(bucket);
	if (!bucketLock.tryLockShared()) {
	    throw new IllegalStateException(
		    "We must ensure that the bucket archiver has a "
			    + "lock to the bucket it will transfer");
	}
	Runnable r = new BucketArchiverRunner(bucketArchiver, bucket,
		bucketLock);
	return r;
    }

    private Bucket createBucketWithErrorHandling(String indexName, String path) {
	Bucket bucket;
	try {
	    bucket = new Bucket(indexName, path);
	} catch (FileNotFoundException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
	return bucket;
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_BUCKET_THAW)
    public void archiveBucket(@QueryParam("index") String index,
	    @QueryParam("from") String from, @QueryParam("to") String to) {
	logMetricsAtEndpoint(ENDPOINT_BUCKET_THAW);
	BucketThawer bucketThawer = BucketThawerFactory.createDefaultThawer();
	bucketThawer.thawBuckets(index, dateFromString(from),
		dateFromString(to));
    }

    private Date dateFromString(String dateAsString) {
	return StringDateConverter.convert(dateAsString);
    }

    private void logMetricsAtEndpoint(String endpoint) {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_ARCHIVER, endpoint);
	ShepMetricsHelper.update(logger, logMessage);
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(ENDPOINT_LIST_BUCKETS)
    public List<BucketBean> listAllBuckets() {
	List<BucketBean> beans = new ArrayList<BucketBean>();
	for (Bucket bucket : listBuckets()) {
	    BucketBean bucketBean = createBeanFromBucket(bucket);
	    beans.add(bucketBean);
	}
	beans.add(new BucketBean("fisk", "disk", "risk", "lisk"));
	return beans;
    }

    private List<Bucket> listBuckets() {
	return listBuckets(null);
    }

    private List<Bucket> listBuckets(String index) {
	ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
		.getConfiguredArchiveFileSystem();
	ArchiveConfiguration archiveConfiguration = ArchiveConfiguration
		.getSharedInstance();
	PathResolver pathResolver = new PathResolver(archiveConfiguration);
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, archiveFileSystem);
	ArchiveBucketsLister archiveBucketsLister = new ArchiveBucketsLister(
		archiveFileSystem, indexesLister, pathResolver);
	BucketFormatChooser bucketFormatChooser = new BucketFormatChooser(
		archiveConfiguration);
	BucketFormatResolver bucketFormatResolver = new BucketFormatResolver(
		pathResolver, archiveFileSystem, bucketFormatChooser);

	List<Bucket> archivedBuckets;
	if (index == null || index.equals("")) {
	    archivedBuckets = archiveBucketsLister.listBuckets();
	} else {
	    archivedBuckets = archiveBucketsLister.listBucketsInIndex(index);
	}
	return bucketFormatResolver.resolveBucketsFormats(archivedBuckets);
    }

    private BucketBean createBeanFromBucket(Bucket bucket) {
	return new BucketBean(bucket.getFormat().name(), bucket.getIndex(),
		bucket.getName(), bucket.getURI().toString());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(ENDPOINT_LIST_BUCKETS + "/{index}")
    public List<BucketBean> listBucketsForIndex(@PathParam("index") String index) {
	List<BucketBean> beans = new ArrayList<BucketBean>();
	for (Bucket bucket : listBuckets(index)) {
	    BucketBean bucketBean = createBeanFromBucket(bucket);
	    beans.add(bucketBean);
	}
	return beans;
    }

}

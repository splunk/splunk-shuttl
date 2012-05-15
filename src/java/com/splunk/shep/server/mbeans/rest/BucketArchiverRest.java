package com.splunk.shep.server.mbeans.rest;

import static com.splunk.shep.ShepConstants.*;
import static com.splunk.shep.archiver.LogFormatter.*;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.FormParam;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.eclipse.jetty.util.ajax.JSON;

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
import com.splunk.shep.archiver.thaw.BucketFilter;
import com.splunk.shep.archiver.thaw.BucketFormatChooser;
import com.splunk.shep.archiver.thaw.BucketFormatResolver;
import com.splunk.shep.archiver.thaw.BucketThawer;
import com.splunk.shep.archiver.thaw.BucketThawer.ThawInfo;
import com.splunk.shep.archiver.thaw.BucketThawerFactory;
import com.splunk.shep.archiver.thaw.StringDateConverter;
import com.splunk.shep.metrics.ShepMetricsHelper;
import com.splunk.shep.server.model.BucketBean;

/**
 * REST endpoints for archiving.
 */
@Path(ENDPOINT_ARCHIVER)
public class BucketArchiverRest {
    private static final org.apache.log4j.Logger logger = Logger
	    .getLogger(BucketArchiverRest.class);


    /**
     * Example on how to archive a bucket with this endpoint:
     * /archiver/bucket/archive?path=/local/Path/To/Bucket
     * 
     * @param path
     *            to the bucket to be archived.
     */
    @POST
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_BUCKET_ARCHIVER)
    public void archiveBucket(@FormParam("path") String path,
	    @FormParam("index") String index) {
	
	logMetricsAtEndpoint(ENDPOINT_BUCKET_ARCHIVER);
	
	logger.info(happened("Received REST request to archive bucket",
		"endpoint", ENDPOINT_BUCKET_ARCHIVER, "index", index, "path",
		path));

	if (path == null) {
	    logger.error(happened("No path was provided."));
	    throw new IllegalArgumentException("path must be specified");
	}

	if (index == null) {
	    logger.error(happened("No index was provided."));
	    throw new IllegalArgumentException("index must be specified");
	}

	archiveBucketOnAnotherThread(index, path);
    }

    /**
     * Thaws a range of buckets in either a specific index or all indexes on the
     * archiving fs.
     * 
     * @param index
     *            Any index that exists in both the archiving filesystem and
     *            splunk. Defaults to all indexes in the archiving fs.
     * @param from
     *            Start date of thawing interval (on the form yyyy-MM-dd).
     *            Defaults to 0001-01-01.
     * @param to
     *            End date of thawing interval (on the form yyyy-MM-dd).
     *            Defaults to 9999-12-31.
     * @return
     */
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Path(ENDPOINT_BUCKET_THAW)
    public String thawBuckets(@FormParam("index") String index,
	    @FormParam("from") String from, @FormParam("to") String to) {

	logger.info(happened("Received REST request to thaw buckets",
		"endpoint", ENDPOINT_BUCKET_THAW, "index", index, "from", from,
		"to", to));

	if (from == null) {
	    logger.info("No from time provided - defaulting to 0001-01-01");
	    from = "0001-01-01";
	}
	if (to == null) {
	    logger.info("No to time provided - defaulting to 9999-12-31");
	    to = "9999-12-31";
	}

	Date fromDate = dateFromString(from);
	Date toDate = dateFromString(to);
	if (fromDate == null || toDate == null) {
	    logger.error(happened("Invalid time interval provided."));
	    throw new IllegalArgumentException(
		    "From and to date must be provided on the form yyyy-DD-mm");
	}

	// thaw
	logMetricsAtEndpoint(ENDPOINT_BUCKET_THAW);
	BucketThawer bucketThawer = BucketThawerFactory.createDefaultThawer();
	List<ThawInfo> thawInfo = bucketThawer.thawBuckets(index, fromDate, toDate);

	return convertThawInfoToJSON(thawInfo);
    }

    /**
     * Converts a list of ThawInfo objects into a 
     * JSON object obeying the following schema:
     * {
     *   "thawed":
     *   {
     *     "type":"array",
     *     "items":
     *     {
     *       "type":"BucketBean"
     *     }
     *   }
     *   "failed":
     *   {
     *     "type":"array",
     *     "items":
     *     {
     *       "type":"object",
     *       "properties":
     *       {
     *         "bucket":
     *         {
     *           "type":"BucketBean"
     *         }
     *         "reason":
     *         {
     *           "type":"string"
     *         }
     *       }
     *     }
     *   }
     * @param thawInfos 
     * @return JSON object conforming to the above schema (as a string).
     */
    private String convertThawInfoToJSON(List<ThawInfo> thawInfos) {
	List<BucketBean> thawedBucketBeans = new ArrayList<BucketBean>();
	List<Map<String, Object>> failedBucketBeans = new ArrayList<Map<String, Object>>();
	ObjectMapper mapper = new ObjectMapper();

	for (ThawInfo info : thawInfos) {
	    switch (info.status) {
	    case THAWED:
		thawedBucketBeans.add(createBeanFromBucket(info.bucket));
		break;
	    case FAILED:
		Map<String, Object> temp = new HashMap<String, Object>();
		temp.put("bucket", createBeanFromBucket(info.bucket));
		temp.put("reason", info.message);
		failedBucketBeans.add(temp);
		break;
	    default:
		throw new RuntimeException("Unexpected enum constant: "
			+ info.status);
	    }
	}
	
	HashMap<String, Object> ret = new HashMap<String, Object>();
	ret.put("thawed", thawedBucketBeans);
	ret.put("failed", failedBucketBeans);
	
	try {
	    return mapper.writeValueAsString(ret);
	} catch (Exception e) {
	    logger.error(did(
		    "attempted to convert thawed/failed buckets to JSON string",
		    e, null));
	    throw new RuntimeException(e);
	}
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(ENDPOINT_LIST_INDEXES)
    public String listAllIndexes() {

	logger.info(happened("Received REST request to list indexes",
		"endpoint", ENDPOINT_LIST_INDEXES));

	ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
		.getConfiguredArchiveFileSystem();
	ArchiveConfiguration archiveConfiguration = ArchiveConfiguration
		.getSharedInstance();
	PathResolver pathResolver = new PathResolver(archiveConfiguration);
	ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
		pathResolver, archiveFileSystem);

	return JSON.getDefault().toJSON(indexesLister.listIndexes());
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path(ENDPOINT_LIST_BUCKETS)
    public List<BucketBean> listBucketsForIndex(
	    @QueryParam("index") String index, @QueryParam("from") String from,
	    @QueryParam("to") String to) {
	logger.info(happened("Received REST request to list buckets",
		"endpoint", ENDPOINT_LIST_BUCKETS, "index", index, "from",
		from, "to", to));

	List<BucketBean> beans = new ArrayList<BucketBean>();
	BucketFilter bucketFilter = new BucketFilter();

	// get buckets by index (or all buckets if index is null)
	List<Bucket> buckets = listBuckets(index);

	if (from == null) {
	    logger.info("No from time provided - defaulting to 0001-01-01");
	    from = "0001-01-01";
	}
	if (to == null) {
	    logger.info("No to time provided - defaulting to 9999-12-31");
	    to = "9999-12-31";
	}

	// attempt to filter by date
	try {
	    Date fromDate = dateFromString(from);
	    Date toDate = dateFromString(to);
	    buckets = bucketFilter.filterBucketsByTimeRange(buckets, fromDate,
		    toDate);
	} catch (Exception e) {
	    logger.error(did("attempted to filter buckets by given date range",
		    e, null, "to", to, "from", from));
	    throw new RuntimeException(e);
	}

	for (Bucket bucket : buckets) {
	    BucketBean bucketBean = createBeanFromBucket(bucket);
	    beans.add(bucketBean);
	}
	return beans;
    }

    private Date dateFromString(String dateAsString) {
	return StringDateConverter.convert(dateAsString);
    }

    private String stringFromDate(Date date) {
	return new SimpleDateFormat("yyyy-MM-dd").format(date).toString();
    }

    private void logMetricsAtEndpoint(String endpoint) {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_ARCHIVER, endpoint);
	ShepMetricsHelper.update(logger, logMessage);
    }

    private void archiveBucketOnAnotherThread(String index, String path) {

	logger.info(will("Attempting to archive bucket", "index", index,
		"path", path));
	Runnable r = createBucketArchiverRunner(index, path);
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
	    logger.error(did(
		    "attempted to create bucket object from existing bucket directory",
		    "bucket directory did not exist",
		    "existing bucket directory", "path", path, "index name ",
		    indexName));
	    throw new RuntimeException(e);
	} catch (FileNotDirectoryException e) {
	    logger.error(did(
		    "attempted to create bucket object from existing bucket",
		    "specified path was a file",
		    "specified path to be a directory", "path", path,
		    "index name ", indexName));
	    throw new RuntimeException(e);
	}
	return bucket;
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
		bucket.getName(), bucket.getURI().toString(),
		stringFromDate(bucket.getEarliest()),
		stringFromDate(bucket.getLatest()),
		FileUtils.byteCountToDisplaySize(bucket.getSize()));
    }
}

// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.server.mbeans.rest;

import static com.splunk.shuttl.ShuttlConstants.*;
import static com.splunk.shuttl.archiver.LogFormatter.*;

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

import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.archive.PathResolver;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.fileSystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ArchiveBucketsLister;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.BucketFormatChooser;
import com.splunk.shuttl.archiver.thaw.BucketFormatResolver;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawer.FailedBucket;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.StringDateConverter;
import com.splunk.shuttl.metrics.ShuttlMetricsHelper;
import com.splunk.shuttl.server.model.BucketBean;

/**
 * REST endpoints for archiving.
 */
@Path(ENDPOINT_ARCHIVER)
public class BucketArchiverRest {
	private static final org.apache.log4j.Logger logger = Logger
			.getLogger(BucketArchiverRest.class);

	/**
	 * Thaws a range of buckets in either a specific index or all indexes on the
	 * archiving fs.
	 * 
	 * @param index
	 *          Any index that exists in both the archiving filesystem and splunk.
	 *          Defaults to all indexes in the archiving fs.
	 * @param from
	 *          Start date of thawing interval (on the form yyyy-MM-dd). Defaults
	 *          to 0001-01-01.
	 * @param to
	 *          End date of thawing interval (on the form yyyy-MM-dd). Defaults to
	 *          9999-12-31.
	 * @return
	 */
	@POST
	@Produces(MediaType.APPLICATION_JSON)
	@Path(ENDPOINT_BUCKET_THAW)
	public String thawBuckets(@FormParam("index") String index,
			@FormParam("from") String from, @FormParam("to") String to) {

		logger.info(happened("Received REST request to thaw buckets", "endpoint",
				ENDPOINT_BUCKET_THAW, "index", index, "from", from, "to", to));

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
		bucketThawer.thawBuckets(index, fromDate, toDate);

		return convertThawInfoToJSON(bucketThawer);
	}

	/**
	 * Converts a list of ThawInfo objects into a JSON object obeying the
	 * following schema: { "thawed": { "type":"array", "items": {
	 * "type":"BucketBean" } } "failed": { "type":"array", "items": {
	 * "type":"object", "properties": { "bucket": { "type":"BucketBean" }
	 * "reason": { "type":"string" } } } }
	 * 
	 * @param thawInfos
	 * @return JSON object conforming to the above schema (as a string).
	 */
	private String convertThawInfoToJSON(BucketThawer bucketThawer) {
		List<BucketBean> thawedBucketBeans = new ArrayList<BucketBean>();
		List<Map<String, Object>> failedBucketBeans = new ArrayList<Map<String, Object>>();
		ObjectMapper mapper = new ObjectMapper();

		for (Bucket bucket : bucketThawer.getThawedBuckets())
			thawedBucketBeans.add(createBeanFromBucket(bucket));

		for (FailedBucket failedBucket : bucketThawer.getFailedBuckets()) {
			Map<String, Object> temp = new HashMap<String, Object>();
			temp.put("bucket", createBeanFromBucket(failedBucket.bucket));
			temp.put("reason", failedBucket.exception.getClass().getSimpleName());
			failedBucketBeans.add(temp);
		}

		HashMap<String, Object> ret = new HashMap<String, Object>();
		ret.put("thawed", thawedBucketBeans);
		ret.put("failed", failedBucketBeans);

		try {
			return mapper.writeValueAsString(ret);
		} catch (Exception e) {
			logger
					.error(did(
							"attempted to convert thawed/failed buckets to JSON string", e,
							null));
			throw new RuntimeException(e);
		}
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path(ENDPOINT_LIST_INDEXES)
	public String listAllIndexes() {

		logger.info(happened("Received REST request to list indexes", "endpoint",
				ENDPOINT_LIST_INDEXES));

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
	public String listBucketsForIndex(@QueryParam("index") String index,
			@QueryParam("from") String from, @QueryParam("to") String to) {
		logger.info(happened("Received REST request to list buckets", "endpoint",
				ENDPOINT_LIST_BUCKETS, "index", index, "from", from, "to", to));

		List<BucketBean> beans = new ArrayList<BucketBean>();
		BucketFilter bucketFilter = new BucketFilter();
		long totalBucketsSize = 0;

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
			buckets = bucketFilter
					.filterBucketsByTimeRange(buckets, fromDate, toDate);
		} catch (Exception e) {
			logger.error(did("attempted to filter buckets by given date range", e,
					null, "to", to, "from", from));
			throw new RuntimeException(e);
		}

		for (Bucket bucket : buckets) {
			BucketBean bucketBean = createBeanFromBucket(bucket);
			beans.add(bucketBean);
			totalBucketsSize += bucket.getSize();
		}

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("buckets_TOTAL_SIZE",
				FileUtils.byteCountToDisplaySize(totalBucketsSize));
		response.put("buckets", beans);

		try {
			return new ObjectMapper().writeValueAsString(response);
		} catch (Exception e) {
			logger.error(did(
					"attempted to convert buckets and their total size to JSON string",
					e, null));
			throw new RuntimeException(e);
		}
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
		ShuttlMetricsHelper.update(logger, logMessage);
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
		if (index == null || index.equals(""))
			archivedBuckets = archiveBucketsLister.listBuckets();
		else
			archivedBuckets = archiveBucketsLister.listBucketsInIndex(index);
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

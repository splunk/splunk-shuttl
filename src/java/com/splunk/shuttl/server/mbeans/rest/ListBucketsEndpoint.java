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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
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
import com.splunk.shuttl.archiver.bucketsize.ArchiveBucketSize;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketSizeResolver;
import com.splunk.shuttl.archiver.thaw.StringDateConverter;
import com.splunk.shuttl.server.model.BucketBean;

/**
 * Endpoint for listing buckets in the archive.
 */
@Path(ENDPOINT_ARCHIVER)
public class ListBucketsEndpoint {
	private static final org.apache.log4j.Logger logger = Logger
			.getLogger(ListBucketsEndpoint.class);

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

		Date fromDate = getValidFromDate(from);
		Date toDate = getValidToDate(to);

		List<Bucket> filteredBucketsAtIndex = getFilteredBucketsAtIndex(index,
				fromDate, toDate);

		List<BucketBean> beans = new ArrayList<BucketBean>();
		long totalBucketsSize = 0;

		for (Bucket bucket : filteredBucketsAtIndex) {
			beans.add(getBucketBean(bucket));
			totalBucketsSize += bucket.getSize() == null ? 0 : bucket.getSize();
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

	private BucketBean getBucketBean(Bucket bucket) {
		BucketSizeResolver bucketSizeResolver = getBucketSizeResolver();
		Bucket bucketWithSize = bucketSizeResolver.resolveBucketSize(bucket);
		return BucketBean.createBeanFromBucket(bucketWithSize);
	}

	private BucketSizeResolver getBucketSizeResolver() {
		return new BucketSizeResolver(ArchiveBucketSize.create(ArchiveConfiguration
				.getSharedInstance()));
	}

	private Date getValidFromDate(String from) {
		if (from == null) {
			logger.info("No from time provided - defaulting to 0001-01-01");
			from = "0001-01-01";
		}
		return StringDateConverter.convert(from);
	}

	private Date getValidToDate(String to) {
		if (to == null) {
			logger.info("No to time provided - defaulting to 9999-12-31");
			to = "9999-12-31";
		}
		return StringDateConverter.convert(to);
	}

	private List<Bucket> getFilteredBucketsAtIndex(String index, Date fromDate,
			Date toDate) {
		ListsBucketsFiltered listsBucketsFiltered = getListsBucketsFiltered();
		if (index == null)
			return listsBucketsFiltered.listFilteredBuckets(fromDate, toDate);
		else
			return listsBucketsFiltered.listFilteredBucketsAtIndex(index, fromDate,
					toDate);
	}

	private ListsBucketsFiltered getListsBucketsFiltered() {
		return ListsBucketsFilteredFactory.create(ArchiveConfiguration
				.getSharedInstance());
	}

}

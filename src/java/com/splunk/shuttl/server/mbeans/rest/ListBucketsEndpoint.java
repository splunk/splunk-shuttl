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

import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.eclipse.jetty.util.ajax.JSON;

import com.splunk.shuttl.archiver.LocalFileSystemPaths;
import com.splunk.shuttl.archiver.archive.ArchiveConfiguration;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystemFactory;
import com.splunk.shuttl.archiver.filesystem.PathResolver;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesLister;
import com.splunk.shuttl.archiver.listers.ListsBucketsFiltered;
import com.splunk.shuttl.archiver.listers.ListsBucketsFilteredFactory;
import com.splunk.shuttl.archiver.metastore.ArchiveBucketSize;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketSizeResolver;

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

		Date fromDate = RestUtil.getValidFromDate(from);
		Date toDate = RestUtil.getValidToDate(to);

		List<Bucket> filteredBucketsAtIndex = getFilteredBucketsAtIndex(index,
				fromDate, toDate);

		List<Bucket> buckets = filteredBucketsAtIndex;
		List<Bucket> bucketsWithSize = new java.util.ArrayList<Bucket>();
		for (Bucket b : buckets)
			bucketsWithSize.add(getBucketWithSize(b));

		return RestUtil.respondWithBuckets(bucketsWithSize);
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

	private static Bucket getBucketWithSize(Bucket bucket) {
		BucketSizeResolver bucketSizeResolver = getBucketSizeResolver();
		return bucketSizeResolver.resolveBucketSize(bucket);
	}

	private static BucketSizeResolver getBucketSizeResolver() {
		ArchiveConfiguration config = ArchiveConfiguration.getSharedInstance();
		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getWithConfiguration(config);
		LocalFileSystemPaths localFileSystemPaths = LocalFileSystemPaths.create();
		return new BucketSizeResolver(ArchiveBucketSize.create(new PathResolver(
				config), archiveFileSystem, localFileSystemPaths));
	}
}

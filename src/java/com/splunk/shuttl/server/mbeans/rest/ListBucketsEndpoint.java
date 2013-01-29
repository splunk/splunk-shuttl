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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.ShuttlConstants;
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
import com.splunk.shuttl.archiver.util.JsonUtils;
import com.splunk.shuttl.server.distributed.RequestOnSearchPeers;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;

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
	public String listAllIndexes() throws JSONException {
		logger.info(happened("Received REST request to list indexes", "endpoint",
				ENDPOINT_LIST_INDEXES));

		ArchiveFileSystem archiveFileSystem = ArchiveFileSystemFactory
				.getConfiguredArchiveFileSystem();
		ArchiveConfiguration archiveConfiguration = ArchiveConfiguration
				.getSharedInstance();
		PathResolver pathResolver = new PathResolver(archiveConfiguration);
		ArchivedIndexesLister indexesLister = new ArchivedIndexesLister(
				pathResolver, archiveFileSystem);

		JSONObject json = JsonUtils.writeKeyValueAsJson(
				JsonObjectNames.INDEX_COLLECTION, indexesLister.listIndexes());
		List<JSONObject> jsons = RequestOnSearchPeers.createGet(
				ENDPOINT_LIST_INDEXES, null, null, null).execute();
		jsons.add(json);

		JSONObject merge = JsonUtils.mergeJsonsWithKeys(jsons,
				JsonObjectNames.INDEX_COLLECTION);
		return uniqifyIndexes(merge).toString();
	}

	private JSONObject uniqifyIndexes(JSONObject json) throws JSONException {
		JSONArray jsonArray = json.getJSONArray(JsonObjectNames.INDEX_COLLECTION);
		Set<String> uniqueIndexes = new HashSet<String>();
		for (int i = 0; i < jsonArray.length(); i++)
			uniqueIndexes.add(jsonArray.getString(i));
		return JsonUtils.writeKeyValueAsJson(JsonObjectNames.INDEX_COLLECTION,
				uniqueIndexes);
	}

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path(ENDPOINT_LIST_BUCKETS)
	public String listBucketsForIndex(@QueryParam("index") String index,
			@QueryParam("from") String from, @QueryParam("to") String to) {
		logger.info(happened("Received REST request to list buckets", "endpoint",
				ENDPOINT_LIST_BUCKETS, "index", index, "from", from, "to", to));

		try {
			return doListBucketsForIndex(index, from, to);
		} catch (Throwable t) {
			logger.error(did("tried to list buckets", t, "to list bucket", "index",
					index, "from", from, "to", to));
			throw new RuntimeException(t);
		}
	}

	private String doListBucketsForIndex(String index, String from, String to)
			throws JSONException {
		Date fromDate = RestUtil.getValidFromDate(from);
		Date toDate = RestUtil.getValidToDate(to);

		List<Bucket> filteredBucketsAtIndex = getFilteredBucketsAtIndex(index,
				fromDate, toDate);

		List<Bucket> bucketsWithSize = new java.util.ArrayList<Bucket>();
		for (Bucket b : filteredBucketsAtIndex)
			bucketsWithSize.add(getBucketWithSize(b));

		JSONObject jsonObject = JsonUtils.writeKeyValueAsJson(
				JsonObjectNames.BUCKET_COLLECTION, bucketsWithSize);

		RequestOnSearchPeers requestOnSearchPeers = RequestOnSearchPeers.createGet(
				ShuttlConstants.ENDPOINT_LIST_BUCKETS, index, from, to);
		List<JSONObject> jsons = requestOnSearchPeers.execute();
		jsons.add(jsonObject);

		return RestUtil.mergeBucketCollectionsAndAddTotalSize(jsons)
				.put(JsonObjectNames.EXCEPTIONS, requestOnSearchPeers.getExceptions())
				.toString();
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

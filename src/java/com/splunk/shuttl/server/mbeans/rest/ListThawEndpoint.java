// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.server.mbeans.rest;

import static com.splunk.shuttl.ShuttlConstants.*;
import static com.splunk.shuttl.archiver.LogFormatter.*;
import static java.util.Arrays.*;

import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.archiver.flush.ThawedBuckets;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesListerFactory;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.model.LocalBucket;
import com.splunk.shuttl.archiver.thaw.BucketFilter;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkIndexesLayer;
import com.splunk.shuttl.archiver.util.JsonUtils;
import com.splunk.shuttl.server.distributed.GetRequestOnSearchPeers;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;

@Path(ENDPOINT_ARCHIVER + ENDPOINT_LIST_THAW)
public class ListThawEndpoint {

	private static final Logger logger = Logger.getLogger(ListThawEndpoint.class);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public String listThawedBuckets(@QueryParam("index") String index,
			@QueryParam("from") String from, @QueryParam("to") String to)
			throws JSONException {
		logger
				.info(happened("Received REST request to list thawed buckets",
						"endpoint", ENDPOINT_LIST_THAW, "index", index, "from", from, "to",
						to));

		try {
			Date earliest = RestUtil.getValidFromDate(from);
			Date latest = RestUtil.getValidToDate(to);
			List<String> indexes;
			if (index == null)
				indexes = ArchivedIndexesListerFactory.create().listIndexes();
			else
				indexes = asList(index);

			List<Bucket> filteredBuckets = filteredBucketsInThaw(indexes, earliest,
					latest);

			JSONObject json = JsonUtils.writeKeyValueAsJson(
					JsonObjectNames.BUCKET_COLLECTION, filteredBuckets);
			List<JSONObject> jsons = new GetRequestOnSearchPeers(ENDPOINT_LIST_THAW,
					index, from, to).execute();
			jsons.add(json);

			return RestUtil.mergeBucketCollectionsAndAddTotalSize(jsons).toString();
		} catch (Exception e) {
			return JsonUtils.writeKeyValueAsJson(JsonObjectNames.ERRORS, asList(e))
					.toString();
		}
	}

	private List<Bucket> filteredBucketsInThaw(List<String> indexes,
			Date earliest, Date latest) throws IllegalIndexException {
		SplunkIndexesLayer splunkIndexesLayer = SplunkIndexedLayerFactory.create();
		List<Bucket> filteredBuckets = new java.util.ArrayList<Bucket>();
		for (String index : indexes) {
			List<LocalBucket> buckets = ThawedBuckets.getBucketsFromThawLocation(
					index, splunkIndexesLayer.getThawLocation(index));
			filteredBuckets.addAll(BucketFilter.filterBuckets(buckets, earliest,
					latest));
		}
		return filteredBuckets;
	}
}

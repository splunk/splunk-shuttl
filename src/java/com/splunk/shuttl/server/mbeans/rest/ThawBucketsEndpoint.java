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

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.BucketThawer;
import com.splunk.shuttl.archiver.thaw.BucketThawer.FailedBucket;
import com.splunk.shuttl.archiver.thaw.BucketThawerFactory;
import com.splunk.shuttl.archiver.thaw.StringDateConverter;
import com.splunk.shuttl.metrics.ShuttlMetricsHelper;
import com.splunk.shuttl.server.model.BucketBean;

/**
 * Endpoint for thawing buckets.
 */
@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_THAW)
public class ThawBucketsEndpoint {

	private static final Logger logger = Logger
			.getLogger(ThawBucketsEndpoint.class);

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

		Date fromDate = StringDateConverter.convert(from);
		Date toDate = StringDateConverter.convert(to);
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

	private void logMetricsAtEndpoint(String endpoint) {
		String logMessage = String.format(
				" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
				ENDPOINT_ARCHIVER, endpoint);
		ShuttlMetricsHelper.update(logger, logMessage);
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
			thawedBucketBeans.add(BucketBean.createBeanFromBucket(bucket));

		for (FailedBucket failedBucket : bucketThawer.getFailedBuckets()) {
			Map<String, Object> temp = new HashMap<String, Object>();
			temp.put("bucket", BucketBean.createBeanFromBucket(failedBucket.bucket));
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
}

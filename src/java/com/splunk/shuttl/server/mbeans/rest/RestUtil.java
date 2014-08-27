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

import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.archiver.thaw.StringDateConverter;
import com.splunk.shuttl.archiver.util.JsonUtils;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;

public class RestUtil {

	private static final Logger logger = Logger.getLogger(RestUtil.class);

	public static Date getValidFromDate(String from) {
		if (from == null) {
			logger.debug("No from time provided - defaulting to 0001-01-01");
			from = "0001-01-01";
		}
		return StringDateConverter.convert(from);
	}

	public static Date getValidToDate(String to) {
		if (to == null) {
			logger.debug("No to time provided - defaulting to 9999-12-31");
			to = "9999-12-31";
		}
		return StringDateConverter.convert(to);
	}

	public static JSONObject mergeBucketCollectionsAndAddTotalSize(
			List<JSONObject> jsons) throws JSONException {
		JSONObject mergedBuckets = JsonUtils.mergeKey(jsons,
				JsonObjectNames.BUCKET_COLLECTION);
		long size = JsonUtils.sumKeyInNestedJson(mergedBuckets,
				JsonObjectNames.SIZE, JsonObjectNames.BUCKET_COLLECTION);

		return JsonUtils.writeKeyValueAsJson(JsonObjectNames.BUCKET_COLLECTION,
				mergedBuckets.get(JsonObjectNames.BUCKET_COLLECTION),
				JsonObjectNames.BUCKET_COLLECTION_SIZE, size);
	}

}

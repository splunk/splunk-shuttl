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

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.thaw.StringDateConverter;
import com.splunk.shuttl.server.model.BucketBean;

public class RestUtil {

	private static final Logger logger = Logger.getLogger(RestUtil.class);

	public static Date getValidFromDate(String from) {
		if (from == null) {
			logger.info("No from time provided - defaulting to 0001-01-01");
			from = "0001-01-01";
		}
		return StringDateConverter.convert(from);
	}

	public static Date getValidToDate(String to) {
		if (to == null) {
			logger.info("No to time provided - defaulting to 9999-12-31");
			to = "9999-12-31";
		}
		return StringDateConverter.convert(to);
	}

	public static String writeMapAsJson(Map<String, Object> ret) {
		try {
			return new ObjectMapper().writeValueAsString(ret);
		} catch (Exception e) {
			logger.error(did("attempted to convert thawed/failed "
					+ "buckets to JSON string", e, null));
			throw new RuntimeException(e);
		}
	}

	/**
	 * @return JSON response with buckets and their total size.
	 */
	public static String respondWithBuckets(List<Bucket> buckets) {
		List<BucketBean> beans = new ArrayList<BucketBean>();
		long totalBucketsSize = 0;

		for (Bucket bucket : buckets) {
			beans.add(getBucketBean(bucket));
			totalBucketsSize += bucket.getSize() == null ? 0 : bucket
					.getSize();
		}

		Map<String, Object> response = new HashMap<String, Object>();
		response.put("buckets_TOTAL_SIZE",
				FileUtils.byteCountToDisplaySize(totalBucketsSize));
		response.put("buckets", beans);

		return RestUtil.writeMapAsJson(response);
	}

	private static BucketBean getBucketBean(Bucket bucket) {
		return BucketBean.createBeanFromBucket(bucket);
	}

	public static String respondWithIndexError(String index) {
		Map<String, Object> responseMap = new HashMap<String, Object>();
		responseMap.put("error", "Could not flush index: " + index
				+ ", because it's not been shuttled.");
		return RestUtil.writeMapAsJson(responseMap);
	}

}

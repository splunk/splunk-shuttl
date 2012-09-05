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

import java.util.Date;
import java.util.Map;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.splunk.shuttl.archiver.thaw.StringDateConverter;

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

}

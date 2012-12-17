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
package com.splunk.shuttl.archiver.util;

import static com.splunk.shuttl.archiver.LogFormatter.*;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class UtilsHttp {

	private static final Logger logger = Logger.getLogger(UtilsHttp.class);

	/**
	 * @param response
	 */
	public static void consumeResponse(HttpResponse response) {
		try {
			if (response != null)
				EntityUtils.consume(response.getEntity());
		} catch (IOException e) {
			logger.error(did(
					"Tried to consume http response of archive bucket request", e,
					"no exception", "response", response));
		}
	}

}

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

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.FormParam;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.amazonaws.util.json.JSONObject;
import com.splunk.shuttl.archiver.flush.Flusher;
import com.splunk.shuttl.archiver.listers.ArchivedIndexesListerFactory;
import com.splunk.shuttl.archiver.thaw.SplunkIndexedLayerFactory;
import com.splunk.shuttl.archiver.util.JsonUtils;
import com.splunk.shuttl.server.distributed.PostRequestOnSearchPeers;
import com.splunk.shuttl.server.mbeans.util.JsonObjectNames;

/**
 * @author petterik
 * 
 */
@Path(ENDPOINT_ARCHIVER + ENDPOINT_BUCKET_FLUSH)
public class FlushEndpoint {

	private static final Logger logger = Logger.getLogger(FlushEndpoint.class);

	@POST
	@Produces(MediaType.APPLICATION_JSON)
	public String flushBuckets(@FormParam("index") final String index,
			@FormParam("from") String from, @FormParam("to") String to) {
		logger.debug(happened("Received REST request to flush buckets", "endpoint",
				ENDPOINT_BUCKET_FLUSH, "index", index, "from", from, "to", to));

		Date fromDate = RestUtil.getValidFromDate(from);
		Date toDate = RestUtil.getValidToDate(to);

		List<Exception> errors = new ArrayList<Exception>();
		Flusher flusher = new Flusher(SplunkIndexedLayerFactory.create());

		List<String> indexes;
		if (index == null)
			indexes = ArchivedIndexesListerFactory.create().listIndexes();
		else
			indexes = asList(index);

		for (String i : indexes) {
			try {
				flusher.flush(i, fromDate, toDate);
			} catch (Exception e) {
				errors.add(e);
			}
		}

		JSONObject json = RestUtil.writeKeyValueAsJson(
				JsonObjectNames.BUCKET_COLLECTION, flusher.getFlushedBuckets(),
				JsonObjectNames.ERRORS, errors);

		List<JSONObject> jsons = new PostRequestOnSearchPeers(
				ENDPOINT_BUCKET_FLUSH, index, from, to).execute();
		jsons.add(json);

		return JsonUtils.mergeJsonsWithKeys(jsons,
				JsonObjectNames.BUCKET_COLLECTION, JsonObjectNames.ERRORS).toString();
	}

}

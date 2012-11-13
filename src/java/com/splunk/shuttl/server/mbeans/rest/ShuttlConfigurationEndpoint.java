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

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path(ENDPOINT_SHUTTL_CONFIGURATION)
public class ShuttlConfigurationEndpoint {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path(ENDPOINT_CONFIG_SERVERNAME)
	public String getServerName() {
		final String serverName = null;
		Map<String, Object> hashMap = new HashMap<String, Object>();
		hashMap.put("server_name", serverName);
		return RestUtil.writeMapAsJson(hashMap);
	}
}

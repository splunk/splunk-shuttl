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

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * A RuntimeException that throws a HTTP error with status code 500 (Internal
 * Server error) and given message
 * 
 * @author kpakkirisamy
 * 
 */
public class ShuttlRestException extends WebApplicationException {
	static final long serialVersionUID = 213;

	public ShuttlRestException(String message) {
		super(Response.status(500).entity(message).type(MediaType.TEXT_PLAIN)
				.build());
	}
}

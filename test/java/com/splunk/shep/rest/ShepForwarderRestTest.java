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
package com.splunk.shep.rest;

import static org.testng.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import org.testng.annotations.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ShepForwarderRestTest {
    // TODO: don't hardcode the default port/host
    private static final String shepHost = "localhost";
    private static final int shepMgmtPort = 9090;
    private static String basePath = "/shep/rest/forwarder";

    @Test(groups = { "integration" })
    public void getSinkPrefixDirectwrite() throws URISyntaxException {
	URI sinkPrefixUri = new URI("http", null, shepHost, shepMgmtPort,
		(basePath + "/sinkprefix"), null, null);
	System.out.println("SinkPrefix URI is " + sinkPrefixUri);
	Client client = Client.create();
	WebResource resource = client.resource(sinkPrefixUri).queryParam(
		"name", "directwrite");
	System.out.println("URI is "+ resource.getURI());
	ClientResponse response = resource.accept(MediaType.TEXT_PLAIN).get(
		ClientResponse.class);
	int status = response.getStatus();
	String sinkPrefix = response.getEntity(String.class);

	assertEquals(status, 200, "Unexpected status, response=" + response);
	assertEquals(sinkPrefix, "/splunkeventdata",
		"Unexpected entity, response=" + response);
    }

    @Test(groups = { "integration" })
    public void getSplunkExportServiceStatus() throws Exception {
	URI exportSrvcUri = new URI("http", null, shepHost, shepMgmtPort,
		(basePath + "/exportservicestatus"), null, null);
	System.out.println("ExportServiceStatus URI is " + exportSrvcUri);
	Client client = Client.create();
	WebResource resource = client.resource(exportSrvcUri);
	System.out.println("URI is " + resource.getURI());
	ClientResponse response = resource.accept(MediaType.TEXT_PLAIN).get(
		ClientResponse.class);
	int status = response.getStatus();
	String srvcStatus = response.getEntity(String.class);
	assertEquals(status, 200, "Unexpected status, response=" + response);
	// TODO update test with real status after calling start() and stop()
	assertEquals(srvcStatus, "it is working",
		"Unexpected entity, response=" + response);
    }

}
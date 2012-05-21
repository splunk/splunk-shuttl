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
package com.splunk.shuttl.rest;

import static org.testng.Assert.*;

import java.net.URI;
import java.net.URISyntaxException;

import javax.ws.rs.core.MediaType;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class ShuttlServerRestTest {
    // TODO: don't hardcode the default port/host
    private static final String shuttlHost = "localhost";
    private static final int shuttlMgmtPort = 9090;
    private static String basePath = "/shuttl/rest/server";
    private Client client;
    
    @BeforeClass(groups = { "integration" })
    public void setUpClient() {
	System.out.println("*** Running ShuttlServerRestTest ***");
	client = Client.create();
	waitForServerToStart();
    }

    private void waitForServerToStart() {
	try {
	    Thread.sleep(5000);
	} catch (InterruptedException e) {
	}
    }

    @Test(groups = { "integration" })
    public void getDefaultHost() throws URISyntaxException {
	URI defaultHostUri = new URI("http", null, shuttlHost, shuttlMgmtPort,
		(basePath + "/defaulthost"), null, null);

	WebResource resource = client.resource(defaultHostUri);
	ClientResponse response = resource.accept(MediaType.TEXT_PLAIN).get(
		ClientResponse.class);
	int status = response.getStatus();
	String defaultHost = response.getEntity(String.class);
	assertEquals(status, 200, "Unexpected status. response=" + response);
	assertEquals(defaultHost, "localhost",
		"Unexpected defaultHost. response="
		+ response);
    }

    @Test(groups = { "integration" })
    public void getDefaultPort() throws URISyntaxException {
	URI defaultPortUri = new URI("http", null, shuttlHost, shuttlMgmtPort,
		(basePath + "/defaultport"), null, null);

	WebResource resource = client.resource(defaultPortUri);
	ClientResponse response = resource.accept(MediaType.TEXT_PLAIN).get(
		ClientResponse.class);
	int status = response.getStatus();
	int defaultPort = Integer.parseInt(response.getEntity(String.class));
	assertEquals(status, 200, "Unexpected status. response=" + response);
	assertEquals(defaultPort, 9000, "Unexpected defaultPort, response="
		+ response);
    }
}
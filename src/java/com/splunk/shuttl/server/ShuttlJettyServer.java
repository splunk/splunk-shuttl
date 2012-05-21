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
package com.splunk.shuttl.server;

import java.io.File;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.eclipse.jetty.xml.XmlConfiguration;

import com.splunk.shuttl.server.mbeans.ShuttlServer;

/**
 * Main class that starts the integrated Jetty server
 * 
 * @author kpakkirisamy
 * 
 */
public class ShuttlJettyServer {
    public static void main(String args[]) {
	org.apache.log4j.Logger logger = Logger.getLogger("ShuttlServer");
	try {
	    Server server = new Server();

	    // TODO: Replace paths relative to /bin/
	    XmlConfiguration configuration = new XmlConfiguration(new File(
		    "../jetty/shuttl.xml").toURI().toURL());
	    configuration.configure(server);

	    // TODO: Replace paths relative to /bin/
	    server.setHandler(new WebAppContext("../webapps/shuttl", "/shuttl"));
	    ShuttlServer servermbean = new ShuttlServer();
	    Connector connectors[] = server.getConnectors();
	    for (Connector c : connectors) {
		logger.debug("Connector Name: " + c.getName());
		logger.debug("  host: " + c.getHost());
		logger.debug("  port: " + c.getPort());
		if (c.getName().equals("Splunk.Shuttl.Http")) {
		    c.setHost(servermbean.getHttpHost());
		    c.setPort(servermbean.getHttpPort());
		}
	    }
	    server.start();
	} catch (Exception e) {
	    logger.error("Error during startup", e);
	    System.exit(1);
	}
    }
}

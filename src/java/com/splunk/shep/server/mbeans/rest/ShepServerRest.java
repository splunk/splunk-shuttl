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
package com.splunk.shep.server.mbeans.rest;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

/**
 * Exposes the Server MBean over REST
 * 
 * @author kpakkirisamy
 * 
 */
@Path("/server")
public class ShepServerRest {
    private org.apache.log4j.Logger logger = Logger.getLogger(getClass());

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/defaulthost")
    public String getDefHadoopClusterHostText() {
	try {
	    return (getAttribute("DefHadoopClusterHost"));
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // for debugging using browsers
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/defaulthost")
    public String getDefHadoopClusterHostHTML() {
	try {
	    return "<html> " + "<title>" + "Shep Rest Endpoint" + "</title>"
		+ "<body><h1>" + getAttribute("DefHadoopClusterHost")
		+ "</body></h1>" + "</html> ";
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path("/defaultport")
    public String getDefHadoopClusterPortText() {
	try {
	    return (getAttribute("DefHadoopClusterPort"));
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // for debugging using browsers
    @GET
    @Produces(MediaType.TEXT_HTML) 
    @Path("/defaultport")
    public String getDefHadoopClusterPortHTML() { 
	try {
	    return "<html> " + "<title>" + "Shep Rest Endpoint" + "</title>"
		+ "<body><h1>" + getAttribute("DefHadoopClusterPort")
		+"</body></h1>" + "</html> "; 
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // hack for HADOOP-254. Remove before GA
    // will generate a warning about a GET being void during runtime
    // TODO
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path("/shutdown")
    public void shutdownHTML() {
	logger.info("Shep shutting down ..");
	System.exit(0);
    }

    private String getAttribute(String attr) throws Exception {
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	ObjectName name = new ObjectName("com.splunk.shep.mbeans:type=Server");
	Object x = mbs.getAttribute(name, attr);
	return (x.toString());
    }

}

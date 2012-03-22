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

import static com.splunk.shep.ShepConstants.*;

import java.lang.management.ManagementFactory;

import javax.management.JMX;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shep.metrics.ShepMetricsHelper;
import com.splunk.shep.server.mbeans.ShepServerMBean;

/**
 * Exposes the Server MBean over REST
 * 
 * @author kpakkirisamy
 * 
 */
// @Path("/server")
@Path(ENDPOINT_SERVER)
public class ShepServerRest {
    private org.apache.log4j.Logger logger = Logger.getLogger(getClass());
    
    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_DEFAULT_HOST)
    public String getDefHadoopClusterHostText() {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_SERVER, ENDPOINT_DEFAULT_HOST);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return (getProxy().getDefHadoopClusterHost());
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // for debugging using browsers
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path(ENDPOINT_DEFAULT_HOST)
    public String getDefHadoopClusterHostHTML() {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_SERVER, ENDPOINT_DEFAULT_HOST);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return "<html> " + "<title>" + "Shep Rest Endpoint" + "</title>"
		    + "<body><h1>" + getProxy().getDefHadoopClusterHost()
		+ "</body></h1>" + "</html> ";
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_DEFAULT_PORT)
    public String getDefHadoopClusterPortText() {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_SERVER, ENDPOINT_DEFAULT_PORT);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return (Integer.toString(getProxy().getDefHadoopClusterPort()));
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // for debugging using browsers
    @GET
    @Produces(MediaType.TEXT_HTML) 
    @Path(ENDPOINT_DEFAULT_PORT)
    public String getDefHadoopClusterPortHTML() { 
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_SERVER, ENDPOINT_DEFAULT_PORT);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return "<html> " + "<title>" + "Shep Rest Endpoint" + "</title>"
		    + "<body><h1>" + getProxy().getDefHadoopClusterPort()
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
    @Path(ENDPOINT_SHUTDOWN)
    public void shutdownHTML() {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_SERVER, ENDPOINT_SHUTDOWN);
	ShepMetricsHelper.update(logger, logMessage);

	logger.info("Shep shutting down ..");
	System.exit(0);
    }

    private ShepServerMBean getProxy() throws Exception {
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	ObjectName name = new ObjectName("com.splunk.shep.mbeans:type=Server");
	ShepServerMBean proxy = (com.splunk.shep.server.mbeans.ShepServerMBean) JMX
		.newMBeanProxy(mbs, name,
			com.splunk.shep.server.mbeans.ShepServerMBean.class);
	return (proxy);
    }

}

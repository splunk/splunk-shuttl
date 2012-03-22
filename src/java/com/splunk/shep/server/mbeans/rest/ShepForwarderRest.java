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
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.apache.log4j.Logger;

import com.splunk.shep.metrics.ShepMetricsHelper;
import com.splunk.shep.server.mbeans.ShepForwarderMBean;


/**
 * Expose forwarder MBean over REST
 * 
 * @author kpakkirisamy
 * 
 */
@Path(ENDPOINT_FORWARDER)
public class ShepForwarderRest {
    private org.apache.log4j.Logger logger = Logger.getLogger("ShepServer");

    @GET
    @Produces(MediaType.TEXT_PLAIN)
    @Path(ENDPOINT_SINK_PREFIX)
    public String getHDFSSinkPrefixText(@QueryParam("name") String name) {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_FORWARDER,
		ENDPOINT_SINK_PREFIX);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return (getHDFSSinkPrefix(name));
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    // for debugging using browsers
    @GET
    @Produces(MediaType.TEXT_HTML)
    @Path(ENDPOINT_SINK_PREFIX)
    public String getHDFSSinkPrefixHTML(@QueryParam("name") String name) {
	String logMessage = String.format(
		" Metrics - group=REST series=%s%s%s call=1", ENDPOINT_CONTEXT,
		ENDPOINT_FORWARDER, ENDPOINT_SINK_PREFIX);
	ShepMetricsHelper.update(logger, logMessage);

	try {
	    return "<html> " + "<title>" + "Shep Rest Endpoint" + "</title>"
		    + "<body><h1>" + getHDFSSinkPrefix(name)
		    + "</body></h1>" + "</html> ";
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepRestException(e.getMessage());
	}
    }

    private String getHDFSSinkPrefix(String name) throws Exception {
	MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
	ObjectName objname = new ObjectName(
		"com.splunk.shep.mbeans:type=Forwarder");
	ShepForwarderMBean proxy = (com.splunk.shep.server.mbeans.ShepForwarderMBean) JMX
		.newMBeanProxy(mbs, objname,
			com.splunk.shep.server.mbeans.ShepForwarderMBean.class);
	return (proxy.getHDFSSinkPrefix(name));
    }

}

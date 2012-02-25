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
package com.splunk.shep.server.mbeans;

import java.io.FileReader;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import com.splunk.shep.server.model.ServerConf;

/**
 * 
 * @author kpakkirisamy
 * 
 */
public class ShepServer implements ShepServerMBean {
    private Logger logger = Logger.getLogger(getClass());
    private String defHadoopClusterHost = null;
    private int defHadoopClusterPort = -1;
    private ArrayList<ServerConf.HadoopCluster> clusterlist;
    private String SERVERCONF_XML = "etc/apps/shep/conf/server.xml";
    ServerConf conf;

    public ShepServer() throws ShepMBeanException {
	try {
	    String splunkhome = System.getProperty("splunk.home");
	    JAXBContext context = JAXBContext.newInstance(ServerConf.class);
	    Unmarshaller um = context.createUnmarshaller();
	    this.conf = (ServerConf) um.unmarshal(new FileReader(
		    splunkhome + SERVERCONF_XML));
	    this.clusterlist = conf.getClusterlist();
	    for (ServerConf.HadoopCluster cluster : this.clusterlist) {
		if (cluster.isDefcluster()) {
		    this.defHadoopClusterHost = cluster.getHost();
		    this.defHadoopClusterPort = cluster.getPort();
		}
	    }
	} catch (Exception e) {
	    logger.error(e);
	    throw new ShepMBeanException(e);
	}
    }

    @Override
    public String getHadoopClusterHost(String name) {
	// TODO Auto-generated method stub
	return null;
    }

    @Override
    public int getHadoopClusterPort(String name) {
	// TODO Auto-generated method stub
	return 0;
    }

    @Override
    public String getDefHadoopClusterHost() throws ShepMBeanException {
	if (this.defHadoopClusterHost == null) {
	    throw new ShepMBeanException("MBean not initialized with xml bean");
	}
	return this.defHadoopClusterHost;
    }

    public void setDefHadoopClusterHost(String defHadoopClusterHost) {
	this.defHadoopClusterHost = defHadoopClusterHost;
    }

    @Override
    public int getDefHadoopClusterPort() throws ShepMBeanException {
	if (this.defHadoopClusterPort == -1) {
	    throw new ShepMBeanException("MBean not initialized with xml bean");
	}
	return this.defHadoopClusterPort;
    }

    public void setDefHadoopClusterPort(int defHadoopClusterPort) {
	this.defHadoopClusterPort = defHadoopClusterPort;
    }

}

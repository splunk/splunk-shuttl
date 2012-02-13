package com.splunk.shep.s2s.forwarder.mbeans;

import java.io.FileReader;
import java.util.ArrayList;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;

import com.splunk.shep.s2s.forwarder.model.ServerConf;

public class ShepServer implements ShepServerMBean {
    private Logger logger = Logger.getLogger(getClass());
    private String defHadoopClusterHost;
    private int defHadoopClusterPort;
    private ArrayList<ServerConf.HadoopCluster> clusterlist;
    private String SERVERCONF_XML = "etc/apps/shep/default/ServerConf.xml";

    public ShepServer() throws Exception {
	logger.info("shepserver");
	try {
	    String splunkhome = System.getProperty("splunk.home");
	    JAXBContext context = JAXBContext.newInstance(ServerConf.class);
	    Unmarshaller um = context.createUnmarshaller();
	    ServerConf conf = (ServerConf) um.unmarshal(new FileReader(
		    splunkhome + SERVERCONF_XML));
	    this.clusterlist = conf.getClusterlist();
	    for (int i = 0; i < this.clusterlist.size(); i++) {
		logger.info("iter shepserver " + i);
		ServerConf.HadoopCluster cluster = this.clusterlist.get(i);
		if (cluster.isDefcluster()) {
		    this.defHadoopClusterHost = cluster.getHost();
		    this.defHadoopClusterPort = cluster.getPort();
		}
	    }
	} catch (Exception e) {
	    logger.info("shepserver exception");
	    e.printStackTrace();
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
    public String getDefHadoopClusterHost() {
	return defHadoopClusterHost;
    }

    public void setDefHadoopClusterHost(String defHadoopClusterHost) {
	this.defHadoopClusterHost = defHadoopClusterHost;
    }

    @Override
    public int getDefHadoopClusterPort() {
	return defHadoopClusterPort;
    }

    public void setDefHadoopClusterPort(int defHadoopClusterPort) {
	this.defHadoopClusterPort = defHadoopClusterPort;
    }

}

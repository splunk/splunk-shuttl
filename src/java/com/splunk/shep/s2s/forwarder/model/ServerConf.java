package com.splunk.shep.s2s.forwarder.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;
import java.util.ArrayList;

@XmlRootElement(namespace = "com.splunk.shep.s2s.forwarder.model")
public class ServerConf {
	private ArrayList<HadoopCluster> clusterlist;
	
	@XmlElementWrapper(name = "clusterlist")
	@XmlElement(name = "hadoopcluster")
	public ArrayList<HadoopCluster> getClusterlist() {
		return clusterlist;
	}

	public void setClusterlist(ArrayList<HadoopCluster> clusterlist) {
		this.clusterlist = clusterlist;
	}

	@XmlRootElement(name = "HadoopCluster")
	@XmlType(propOrder = { "name", "defcluster", "host", "port" })
	public static class HadoopCluster {
		private String host;
		private String name;
		private int port;
		private boolean defcluster;
		
		public boolean isDefcluster() {
			return defcluster;
		}
		public void setDefcluster(boolean defcluster) {
			this.defcluster = defcluster;
		}
		public int getPort() {
			return port;
		}
		public void setPort(int port) {
			this.port = port;
		}
		public String getHost() {
			return host;
		}
		public void setHost(String host) {
			this.host = host;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
	}

}

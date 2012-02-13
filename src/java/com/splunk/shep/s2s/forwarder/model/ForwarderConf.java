package com.splunk.shep.s2s.forwarder.model;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlElementWrapper;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlType;

import java.util.ArrayList;

@XmlRootElement(namespace = "com.splunk.shep.s2s.forwarder.model")
@XmlType(propOrder = { "hdfssinklist", "flumesinklist" })
public class ForwarderConf {

	private ArrayList<HDFSSink> hdfssinklist;
	
	
	private ArrayList<FlumeSink> flumesinklist;
	
	@XmlElementWrapper(name = "hdfssinklist")
	@XmlElement(name = "hdfssink")
	public ArrayList<HDFSSink> getHdfssinklist() {
		return hdfssinklist;
	}

	public void setHdfssinklist(ArrayList<HDFSSink> hdfssinklist) {
		this.hdfssinklist = hdfssinklist;
	}

	@XmlElementWrapper(name = "flumesinklist")
	@XmlElement(name = "flumesink")
	public ArrayList<FlumeSink> getFlumesinklist() {
		return flumesinklist;
	}

	public void setFlumesinklist(ArrayList<FlumeSink> flumesinklist) {
		this.flumesinklist = flumesinklist;
	}

	@XmlRootElement(name = "HDFSSink")
	@XmlType(propOrder = { "name", "cluster", "fileprefix", "maxEventSizeKB", "useAppending" })
	public static class HDFSSink {
		private String cluster;
		private String name;
		private String fileprefix;
		private int maxEventSizeKB=64000;
		private boolean useAppending;
		
		public int getMaxEventSizeKB() {
			return maxEventSizeKB;
		}
		public void setMaxEventSizeKB(int maxEventSizeKB) {
			this.maxEventSizeKB = maxEventSizeKB;
		}
		public boolean isUseAppending() {
			return useAppending;
		}
		public void setUseAppending(boolean useAppending) {
			this.useAppending = useAppending;
		}
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getCluster() {
			return cluster;
		}
		public void setCluster(String cluster) {
			this.cluster = cluster;
		}
		public String getFileprefix() {
			return fileprefix;
		}
		public void setFileprefix(String fileprefix) {
			this.fileprefix = fileprefix;
		}
	}
	
	@XmlRootElement(name = "FlumeSink")
	@XmlType(propOrder = { "name", "cluster" })
	public static class FlumeSink {
		private String name;
		private String cluster;
		public String getName() {
			return name;
		}
		public void setName(String name) {
			this.name = name;
		}
		public String getCluster() {
			return cluster;
		}
		public void setCluster(String cluster) {
			this.cluster = cluster;
		}
	}

}

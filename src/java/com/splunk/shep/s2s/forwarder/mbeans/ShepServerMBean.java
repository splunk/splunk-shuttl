package com.splunk.shep.s2s.forwarder.mbeans;

public interface ShepServerMBean {

    public String getDefHadoopClusterHost();

    public int getDefHadoopClusterPort();

    public String getHadoopClusterHost(String name);

    public int getHadoopClusterPort(String name);
}

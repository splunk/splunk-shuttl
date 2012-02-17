package com.splunk.shep.s2s.forwarder.mbeans;

public interface ShepForwarderMBean {

    public String getHDFSSinkPrefix(String name);

    public int getHDFSSinkMaxEventSize(String name);

    public boolean getHDFSSinkUseAppending(String name);

}

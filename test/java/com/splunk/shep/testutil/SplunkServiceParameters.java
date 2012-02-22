package com.splunk.shep.testutil;

import com.splunk.Service;

/**
 * Container for the parameters needed to create and login to a {@link Service} <br>
 * Use convenience method {@link SplunkServiceParameters#getLoggedInService()}
 * to get a service to do Splunk operations on.
 */
public class SplunkServiceParameters {
    public final String username;
    public final String password;
    public final String host;
    public final int mgmtPort;

    public SplunkServiceParameters(String username, String password,
	    String host, String mgmtPort) {
	this.username = username;
	this.password = password;
	this.host = host;
	this.mgmtPort = Integer.parseInt(mgmtPort);
    }

    public String getUsername() {
	return username;
    }

    public String getPassword() {
	return password;
    }

    public String getHost() {
	return host;
    }

    public int getMgmtPort() {
	return mgmtPort;
    }

    @Override
    public String toString() {
	return "SplunkParameters [username=" + username + ", password="
		+ password + ", host=" + host + ", mgmtPort=" + mgmtPort + "]";
    }

    public Service getLoggedInService() {
	Service service = new Service(host, mgmtPort);
	service.login(username, password);
	return service;
    }

}
// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.testutil;

import com.splunk.Args;
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
    public final String appContext;

    public SplunkServiceParameters(String username, String password,
	    String host, String mgmtPort) {
	this.username = username;
	this.password = password;
	this.host = host;
	this.mgmtPort = Integer.parseInt(mgmtPort);
	this.appContext = null;
    }

    public SplunkServiceParameters(String username, String password,
	    String host, String mgmtPort, String appContext) {
	this.username = username;
	this.password = password;
	this.host = host;
	this.mgmtPort = Integer.parseInt(mgmtPort);
	this.appContext = appContext;
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

    public String getAppContext() {
	return appContext;
    }

    @Override
    public String toString() {
	return "SplunkParameters [username=" + username + ", password="
		+ password + ", host=" + host + ", mgmtPort=" + mgmtPort
		+ ", appContext=" + appContext + "]";
    }

    public Service getLoggedInService() {
	Args args = new Args();
	args.put("host", host);
	args.put("port", mgmtPort);
	// args.put("owner", "nobody");
	if (appContext != null) {
	    args.put("app", appContext);
	}

	Service service = new Service(args);
	service.login(username, password);
	return service;
    }

}
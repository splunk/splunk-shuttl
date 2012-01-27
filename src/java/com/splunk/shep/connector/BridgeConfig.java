// BridgeConfig.java
//
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

package com.splunk.shep.connector;

public class BridgeConfig implements AcceptorConfig {
    private String bindIP = "0.0.0.0";
    private int inPort = 9998;

    private boolean usingFlume = true;

    private String outIP = "localhost";
    private int outPort = 8888;
    private String outPath = "/spl/output";
    private long fileRollingSize = 10000000;
    private long maxEventSize = 32000;

    private boolean usingAppend = false;

    public BridgeConfig() {
    }

    public BridgeConfig(String bindIp, int bindPort, String tarIP, int tarPort,
	    long eventSize) {
	setConnectionParams(bindIp, bindPort, tarIP, tarPort, eventSize);
    }

    public void setHDFSMode(boolean directToHDFS) {
	if (directToHDFS)
	    usingFlume = false;
    }

    public void setAppend(boolean useAppend) {
	usingAppend = useAppend;
    }

    public void setConnectionParams(String bindIp, int bindPort, String tarIP,
	    int tarPort, long eventSize) {
	bindIP = bindIp;
	inPort = bindPort;
	outIP = tarIP;
	outPort = tarPort;
	maxEventSize = eventSize;
    }

    public void setConnectionParams(String bindIp, int bindPort, String tarIP,
	    int tarPort, String tarPath, long fileSize) {
	bindIP = bindIp;
	inPort = bindPort;
	outIP = tarIP;
	outPort = tarPort;
	outPath = tarPath;
	fileRollingSize = fileSize;
    }

    public boolean useFlume() {
	return usingFlume;
    }

    public boolean useAppending() {
	return usingAppend;
    }

    public String getBindIP() {
	return bindIP;
    }

    public int getPort() {
	return inPort;
    }

    public String getOutIP() {
	return outIP;
    }

    public int getOutPort() {
	return outPort;
    }

    public String getOutPath() {
	return outPath;
    }

    public long getFileSize() {
	return fileRollingSize;
    }

    public long getEventSize() {
	return maxEventSize;
    }
}

// ShepConf.java
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

/**
 * @author xli
 *
 */

package com.splunk.shep.cli;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Needs a config file, ShepCLI.conf, of the format: HadoopIP=localhost
 * HadoopPort=50001 HadoopHome=/Users/xli/app/hadoop-0.20.2-cdh3u1
 * SplunkHome=/Users/xli/install/splunk43
 */
public class ShepConf 
{
    private String hadoopIP = new String("localhost");
    private String hadoopPort = new String("9000");
    private String hadoopHome = null;
    private String splunkHome = null;
	
	//private Logger logger = Logger.getLogger(getClass());
	
    public ShepConf(String filePath) {
	try {
	    parseConfig(filePath);
	} catch (Exception e) {
	    System.out.println("WARN: Failed opening/parsing ShepCLI.conf.");
	    e.printStackTrace();
	}
    }

    private void parseConfig(String filePath) throws FileNotFoundException,
	    IOException {
	Properties prop = new Properties();
	prop.load(new FileInputStream(filePath));
		
	String ip = prop.getProperty("HadoopIP");
	String port = prop.getProperty("HadoopPort");
	String hdfs = prop.getProperty("HadoopHome");
	String spl = prop.getProperty("SplunkHome");
		
	if (ip != null) {
	    hadoopIP = ip;
	}
 else {
	    System.err.println("WARN: Hadoop host not configured.");
	}
		
	if (port != null) {
	    hadoopPort = port;
	}
 else {
	    System.err.println("WARN: Hadoop port not configured.");
	}
		
	if (hdfs != null) {
	    hadoopHome = hdfs;
	}
 else {
	    System.err.println("WARN: Hadoop home not configured.");
	}

	if (spl != null) {
	    splunkHome = spl;
	}
 else {
	    System.err.println("WARN: Splunk home not configured.");
	}
    }

    public String getHadoopIP() {
	return hadoopIP;
    }

    public void setHadoopIP(String ip) {
	hadoopIP = ip;
    }

    public String getHadoopPort() {
	return hadoopPort;
    }

    public String getHadoopHome() throws Exception {
	if (hadoopHome == null)
	    throw (new Exception("Hadoop home not configured."));
	return hadoopHome;
    }

    public void setHadoopHome(String hdfs) {
	hadoopHome = hdfs;
    }

    public String getSplunkHome() throws Exception {
	if (splunkHome == null)
	    throw (new Exception("Splunk home not configured."));
	return splunkHome;
    }
}

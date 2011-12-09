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
    private String hadoopPort = new String("50001");
    private String hadoopHome = null;
    private String splunkHome = new String("");
	
	//private Logger logger = Logger.getLogger(getClass());
	
    public ShepConf(String filePath) {
	try {
	    parseConfig(filePath);
	} catch (Exception e) {
	    System.out.println("WARN: Failed opening/parsing ShepCLI.conf.");
	    // e.printStackTrace();
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
		
	if (port != null) {
	    hadoopPort = port;
	}
		
	if (hdfs != null) {
	    hadoopHome = hdfs;
	}

	if (spl != null) {
	    splunkHome = spl;
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

    public String getHadoopHome() {
	return hadoopHome;
    }

    public void setHadoopHome(String hdfs) {
	hadoopHome = hdfs;
    }

    public String getSplunkHome() {
	return splunkHome;
    }
}

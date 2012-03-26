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
package com.splunk.shep.server.mbeans;

import static org.testng.Assert.*;

import java.io.File;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * White box testing of MBeans
 * 
 * @author kpakkirisamy
 * 
 */
public class ShepServerMBeanTest {
    private static final int TEST_CLUSTER_PORT = 9797;
    private static final String TEST_CLUSTER_HOST = "test_cluster_host";
    private static final String TEST_CLUSTER_NAME = "test_cluster_name";
    private static final String TMP_SERVER_XML = "/tmp/server.xml";
    private static final String TESTSPLUNKCLUSTER = "testsplunkcluster";
    private static final int TESTPORT = 9090;
    private static final String TESTHOST = "testhost";
    private ShepServerMBean serverMBean = null;
    
    @BeforeClass(groups = { "fast-unit" })
    public void createMBean() throws Exception {
	try {
	    System.out.println("ShepServerMBeanTest - BeforeMethod");
	    this.serverMBean = new ShepServer(TMP_SERVER_XML);
	    this.serverMBean.setHttpHost(TESTHOST);
	    this.serverMBean.setHttpPort(TESTPORT);
	    this.serverMBean.setSplunkClusterName(TESTSPLUNKCLUSTER);
	    this.serverMBean.save();
	    this.serverMBean.refresh();
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new Exception(e);
	}
    }

    @Test(groups = { "fast-unit" })
    public void test_httphost() {
	String httphost = this.serverMBean.getHttpHost();
	assertEquals(httphost, TESTHOST,
		"Unable to save and re-read HttpHost. Expected =  " + TESTHOST
			+ " Actual = " + httphost);
    }

    @Test(groups = { "fast-unit" })
    public void test_httpport() {
	int httpport = this.serverMBean.getHttpPort();
	assertEquals(httpport, TESTPORT,
		"Unable to save and re-read HttpPort. Expected =  " + TESTPORT
			+ " Actual = " + httpport);
    }

    @Test(groups = { "fast-unit" })
    public void test_getSplunkClusterName() {
	String clustername = this.serverMBean.getSplunkClusterName();
	assertEquals(clustername, TESTSPLUNKCLUSTER,
		"Unable to save and re-read SplunkClusterName. Expected =  "
			+ TESTSPLUNKCLUSTER + " Actual = " + clustername);
    }

    @Test(groups = { "fast-unit" })
    public void test_getShepHostName() throws ShepMBeanException {
	String hostname = this.serverMBean.getShepHostName();
	if (hostname.length() < 1) {
	    throw new ShepMBeanException("Invalid ShepHostName " + hostname);
	}
    }

    @Test(groups = { "fast-unit" })
    public void test_addCluster() throws ShepMBeanException {
	this.serverMBean.addHadoopCluster(TEST_CLUSTER_NAME);
	this.serverMBean.setHadoopClusterHost(TEST_CLUSTER_NAME,
		TEST_CLUSTER_HOST);
	this.serverMBean.setHadoopClusterPort(TEST_CLUSTER_NAME,
		TEST_CLUSTER_PORT);
	this.serverMBean.setDefault(TEST_CLUSTER_NAME);
	this.serverMBean.save();
	this.serverMBean.refresh();
	String host1 = this.serverMBean.getDefHadoopClusterHost();
	String host2 = this.serverMBean.getHadoopClusterHost(TEST_CLUSTER_NAME);
	assertEquals(host1, TEST_CLUSTER_HOST);
	assertEquals(host2, TEST_CLUSTER_HOST);
	int port1 = this.serverMBean.getDefHadoopClusterPort();
	int port2 = this.serverMBean.getHadoopClusterPort(TEST_CLUSTER_NAME);
	assertEquals(port1, TEST_CLUSTER_PORT);
	assertEquals(port2, TEST_CLUSTER_PORT);
	boolean defcluster = this.serverMBean.isDefault(TEST_CLUSTER_NAME);
	assertEquals(defcluster, true);
	this.serverMBean.deleteHadoopCluster(TEST_CLUSTER_NAME);
    }

    @Test(groups = { "fast-unit" }, expectedExceptions = ShepMBeanException.class)
    public void test_deleteCluster() throws ShepMBeanException {
	this.serverMBean.addHadoopCluster(TEST_CLUSTER_NAME);
	this.serverMBean.save();
	this.serverMBean.refresh();
	this.serverMBean.deleteHadoopCluster(TEST_CLUSTER_NAME);
	// the following method should throw an exception
	this.serverMBean.getHadoopClusterHost(TEST_CLUSTER_NAME);
	// unreached
    }

    @AfterClass(groups = { "fast-unit" })
    public void destroyMBeanBackingXMLFile() {
	try {
	    new File(TMP_SERVER_XML).delete();
	} catch (Exception e) {
	    // cleanup - ignore exception
	}
    }

}

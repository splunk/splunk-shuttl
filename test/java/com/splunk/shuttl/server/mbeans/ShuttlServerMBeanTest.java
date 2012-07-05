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
package com.splunk.shuttl.server.mbeans;

import static org.testng.Assert.*;

import java.io.File;
import java.io.FileWriter;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * White box testing of MBeans
 * 
 * @author kpakkirisamy
 * 
 */
@Test(groups = { "fast-unit" })
public class ShuttlServerMBeanTest {
	private static final String EMPTY_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
			+ "<ns2:serverConf xmlns:ns2=\"com.splunk.shuttl.server.model\"></ns2:serverConf>";
	private static final int TESTPORT = 9090;
	private static final String TESTHOST = "testhost";
	private ShuttlServerMBean serverMBean = null;

	@BeforeMethod
	public void createMBean() throws Exception {
		try {
			File confFile = getTempFile();
			System.out.println("ShuttlServerMBeanTest - running "
					+ confFile.getPath());
			this.serverMBean = new ShuttlServer(confFile.getPath());
			this.serverMBean.setHttpHost(TESTHOST);
			this.serverMBean.setHttpPort(TESTPORT);
			this.serverMBean.save();
			this.serverMBean.refresh();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		}
	}

	private File getTempFile() throws Exception {
		File confFile = File.createTempFile("shuttlServerMBeanTest", ".xml");
		confFile.deleteOnExit();
		FileWriter writer = new FileWriter(confFile);
		writer.write(EMPTY_XML);
		writer.close();
		return confFile;
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

}

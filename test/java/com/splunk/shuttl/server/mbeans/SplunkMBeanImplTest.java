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

import static com.splunk.shuttl.testutil.TUtilsFile.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.xml.sax.SAXException;

import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsString;

@Test(groups = { "fast-unit" })
public class SplunkMBeanImplTest {

	private SplunkMBeanImpl splunkMBeanImpl;
	private String configurationWithTheTestFields;
	private String password;
	private String username;
	private String port;
	private String host;

	@BeforeMethod
	public void setUp() {
		host = "theHost";
		port = "thePort";
		username = "theUsername";
		password = "thePassword";

		configurationWithTheTestFields = TUtilsMBean.XML_HEADER
				+ "<ns2:splunkConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "<host>" + host + "</host>\n" + "<port>" + port + "</port>\n"
				+ "<username>" + username + "</username>\n" + "<password>" + password
				+ "</password>\n" + "</ns2:splunkConf>\n";
	}

	public void constructor_givenXmlConfFileWithValues_gettersReturnTheValuesConfigured()
			throws IOException {
		File confFile = createFile();
		FileUtils.writeStringToFile(confFile, configurationWithTheTestFields);
		splunkMBeanImpl = new SplunkMBeanImpl(confFile);

		assertEquals(host, splunkMBeanImpl.getHost());
		assertEquals(port, splunkMBeanImpl.getPort());
		assertEquals(username, splunkMBeanImpl.getUsername());
		assertEquals(password, splunkMBeanImpl.getPassword());
	}

	public void constructor_givenEmptyConfiguration_allValuesReturnNull()
			throws IOException {
		splunkMBeanImpl = new SplunkMBeanImpl(
				TUtilsMBean.createEmptyInNamespace("splunkConf"));
		assertNull(splunkMBeanImpl.getHost());
		assertNull(splunkMBeanImpl.getPort());
		assertNull(splunkMBeanImpl.getUsername());
		assertNull(splunkMBeanImpl.getPassword());
	}

	public void save_settingValuesOnEmptyConfig_savesValuesToTheConfFile()
			throws ShuttlMBeanException, ParserConfigurationException, SAXException,
			IOException {
		File confFile = TUtilsMBean.createEmptyInNamespace("splunkConf");
		splunkMBeanImpl = new SplunkMBeanImpl(confFile);
		splunkMBeanImpl.setHost(host);
		splunkMBeanImpl.setPort(port);
		splunkMBeanImpl.setUsername(username);
		splunkMBeanImpl.setPassword(password);

		splunkMBeanImpl.save();

		String confAfterSave = FileUtils.readFileToString(confFile);
		assertEquals(TUtilsString.noSpaces(configurationWithTheTestFields),
				TUtilsString.noSpaces(confAfterSave));
	}

}

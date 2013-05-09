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
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;
import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl;
import com.splunk.shuttl.archiver.filesystem.hadoop.HdfsProperties;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * Test that makes sure that the configuration still supports ArchiveRootURI.
 */
@Test(groups = { "end-to-end" })
public class ShuttlArchiverMBeanArchiverRootURITest {

	ShuttlArchiverMBean archiverMBean;

	@Parameters(value = { "splunk.home", "hadoop.host", "hadoop.port" })
	public void _givenArchiverRootURIWithHdfsScheme_setsBackendAndPathWithURIAndHadoopProperties(
			final String splunkHome, final String hadoopHost, final String hadoopPort) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				try {
					TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
					set_Backend_ArchivePath_AndHadoopProperties(hadoopHost, hadoopPort);
				} catch (IOException e) {
					TUtilsTestNG.failForException("Got IOException", e);
				}
			}
		});
	}

	private void set_Backend_ArchivePath_AndHadoopProperties(String hadoopHost,
			String hadoopPort) throws IOException {
		File hdfsPropertiesFile = BackendConfigurationFiles.create().getByName(
				HdfsProperties.HDFS_PROPERTIES_FILENAME);
		try {
			runHdfsTestCase(hdfsPropertiesFile);
		} finally {
			teardown(hadoopHost, hadoopPort, hdfsPropertiesFile);
		}
	}

	private void runHdfsTestCase(File hdfsPropertiesFile) throws IOException {
		String host = "thehost";
		String port = "9876";
		String archiverRootURI = createHdfsArchiverRootUri(host, port);

		createArchiverMbeanWithArchiverRootURI(archiverRootURI);
		assertEquals("hdfs", archiverMBean.getBackendName());
		assertEquals("/archiver_root", archiverMBean.getArchivePath());

		Properties hdfsProperties = new Properties();
		hdfsProperties.load(FileUtils.openInputStream(hdfsPropertiesFile));
		assertEquals(host, hdfsProperties.getProperty("hadoop.host"));
		assertEquals(port, hdfsProperties.getProperty("hadoop.port"));
	}

	private void teardown(String hadoopHost, String hadoopPort,
			File hdfsPropertiesFile) throws IOException {
		createArchiverMbeanWithArchiverRootURI(createHdfsArchiverRootUri(
				hadoopHost, hadoopPort));
		Properties hdfsProperties = new Properties();
		hdfsProperties.load(FileUtils.openInputStream(hdfsPropertiesFile));
		assertEquals(hadoopHost, hdfsProperties.getProperty("hadoop.host"));
		assertEquals(hadoopPort, hdfsProperties.getProperty("hadoop.port"));
	}

	private String createHdfsArchiverRootUri(String host, String port) {
		return "hdfs://" + host + ":" + port + "/archiver_root";
	}

	private void createArchiverMbeanWithArchiverRootURI(String archiverRootURI)
			throws IOException {
		String configFilePreset = TUtilsMBean.XML_HEADER
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "    <archiverRootURI>" + archiverRootURI + "</archiverRootURI>\n"
				+ "</ns2:archiverConf>";
		File file = createFile();
		file.deleteOnExit();
		FileUtils.writeStringToFile(file, configFilePreset);
		archiverMBean = ShuttlArchiver.createWithConfFile(file);
	}

	@Parameters(value = { "splunk.home" })
	public void _givenArchiverRootURIWithS3Scheme_setsBackendAndPathWithURIAndAmazonProperties(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				try {
					TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
					set_Backend_archivePath_andAmazonProperties();
				} catch (IOException e) {
					TUtilsTestNG.failForException("Got IOException", e);
				}
			}
		});
	}

	private void set_Backend_archivePath_andAmazonProperties() throws IOException {
		String id = "theId";
		String secret = "theSecret";
		String bucket = "theBucket";
		String archiverRootURI = "s3n://" + id + ":" + secret + "@" + bucket
				+ "/archiver_root";

		createArchiverMbeanWithArchiverRootURI(archiverRootURI);
		assertEquals("s3n", archiverMBean.getBackendName());
		assertEquals("/archiver_root", archiverMBean.getArchivePath());

		Properties amazonProperties = new Properties();
		File amazonPropertiesFile = BackendConfigurationFiles.create().getByName(
				AWSCredentialsImpl.AMAZON_PROPERTIES_FILENAME);
		amazonProperties.load(FileUtils.openInputStream(amazonPropertiesFile));
		assertEquals(id, amazonProperties.getProperty("aws.id"));
		assertEquals(secret, amazonProperties.getProperty("aws.secret"));
		assertEquals(bucket, amazonProperties.getProperty("s3.bucket"));
	}

	@Test(groups = { "fast-unit" })
	public void _givenArchiverRootURIWithFileScheme_setsBackendNameAndArchivePath()
			throws IOException {
		createArchiverMbeanWithArchiverRootURI("file:/archiver_root");
		assertEquals("local", archiverMBean.getBackendName());
		assertEquals("/archiver_root", archiverMBean.getArchivePath());
	}

}

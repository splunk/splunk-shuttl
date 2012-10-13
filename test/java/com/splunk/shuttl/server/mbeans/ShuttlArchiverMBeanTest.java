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
import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.filesystem.BackendConfigurationFiles;
import com.splunk.shuttl.archiver.filesystem.glacier.AWSCredentialsImpl;
import com.splunk.shuttl.archiver.filesystem.hadoop.HdfsProperties;
import com.splunk.shuttl.testutil.TUtilsEnvironment;
import com.splunk.shuttl.testutil.TUtilsMBean;
import com.splunk.shuttl.testutil.TUtilsString;
import com.splunk.shuttl.testutil.TUtilsTestNG;

/**
 * White box testing of MBeans
 */
public class ShuttlArchiverMBeanTest {
	ShuttlArchiver archiverMBean;
	private String clusterName;
	private String serverName;
	private String backendName;
	private String archivePath;

	@BeforeMethod(groups = { "fast-unit" })
	public void createMBean() throws Exception {
		File confFile = getTempFile();
		System.out.println("ShuttlArchiverMBeanTest - running "
				+ confFile.getPath());
		archiverMBean = ShuttlArchiver.createWithConfFile(confFile);
		clusterName = "cluster_name";
		serverName = "server_name";
		backendName = "backend";
		archivePath = "/archive_path";
	}

	@Test(groups = { "fast-unit" })
	public void setBackendName_backendNameIsSet_gotBackendName() {
		assertNotEquals(archiverMBean.getBackendName(), backendName);
		archiverMBean.setBackendName(backendName);
		assertEquals(archiverMBean.getBackendName(), backendName);
	}

	@Test(groups = { "fast-unit" })
	public void setArchivePath_archivePathIsSet_gotArchivePath() {
		assertNotEquals(archiverMBean.getArchivePath(), archivePath);
		archiverMBean.setArchivePath(archivePath);
		assertEquals(archiverMBean.getArchivePath(), archivePath);
	}

	@Test(groups = { "fast-unit" })
	public void setArchiveFormat_archiveFormatIsSet_gotArchiveFormat() {
		List<String> archiveFormats1 = asList("SPLUNK_BUCKET");
		assertNotEquals(archiverMBean.getArchiveFormats(), archiveFormats1);
		archiverMBean.setArchiveFormats(archiveFormats1);
		assertEquals(archiverMBean.getArchiveFormats(), archiveFormats1);
	}

	@Test(groups = { "fast-unit" })
	public void setClusterName_clusterNameIsSet_gotClusterName() {
		assertNotEquals(archiverMBean.getClusterName(), clusterName);
		archiverMBean.setClusterName(clusterName);
		assertEquals(archiverMBean.getClusterName(), clusterName);
	}

	@Test(groups = { "fast-unit" })
	public void setServerName_serverNameIsSet_gotCluserName() {
		assertNotEquals(archiverMBean.getServerName(), serverName);
		archiverMBean.setServerName(serverName);
		assertEquals(archiverMBean.getServerName(), serverName);
	}

	@Test(groups = { "fast-unit" })
	public void setBucketFormatPriority_priorityIsSet_gotPriority() {
		List<String> bucketFormatPriority = Arrays.asList("SPLUNK_BUCKET",
				"UNKNOWN");
		assertNotEquals(archiverMBean.getBucketFormatPriority(),
				bucketFormatPriority);
		archiverMBean.setBucketFormatPriority(bucketFormatPriority);
		assertEquals(archiverMBean.getBucketFormatPriority(), bucketFormatPriority);
	}

	@Test(groups = { "fast-unit" })
	public void save_configured_producesCorrectXML() throws Exception {
		List<String> archiveFormats = asList("SPLUNK_BUCKET", "CSV");
		String expectedConfigFile = TUtilsMBean.XML_HEADER
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "<archiveFormats>\n"
				+ "<archiveFormat>SPLUNK_BUCKET</archiveFormat>\n"
				+ "<archiveFormat>CSV</archiveFormat>\n" + "</archiveFormats>\n"
				+ "<clusterName>" + clusterName + "</clusterName>\n" + "<serverName>"
				+ serverName + "</serverName>\n" + "<bucketFormatPriority>"
				+ "SPLUNK_BUCKET" + "</bucketFormatPriority>\n"
				+ "<bucketFormatPriority>" + "CSV" + "</bucketFormatPriority>\n"
				+ "<backendName>" + backendName + "</backendName>\n" + "<archivePath>"
				+ archivePath + "</archivePath>\n" + "</ns2:archiverConf>\n";

		File file = getTempFile();
		archiverMBean = ShuttlArchiver.createWithConfFile(file);
		archiverMBean.setArchiveFormats(archiveFormats);
		archiverMBean.setClusterName(clusterName);
		archiverMBean.setServerName(serverName);
		archiverMBean.setBackendName(backendName);
		archiverMBean.setArchivePath(archivePath);
		archiverMBean.setBucketFormatPriority(archiveFormats);
		archiverMBean.save();

		assertEquals(TUtilsString.noSpaces(FileUtils.readFileToString(file)),
				TUtilsString.noSpaces(expectedConfigFile));
	}

	@Test(groups = { "fast-unit" })
	public void load_preconfiguredFile_givesCorrectValues() throws Exception {
		String configFilePreset = TUtilsMBean.XML_HEADER
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "<archiveFormats>\n"
				+ "<archiveFormat>SPLUNK_BUCKET</archiveFormat>\n"
				+ "<archiveFormat>CSV</archiveFormat>\n" + "</archiveFormats>\n"
				+ "<clusterName>" + clusterName + "</clusterName>\n"
				+ "    <serverName>" + serverName + "</serverName>\n"
				+ "    <backendName>" + backendName + "</backendName>\n"
				+ "    <archivePath>" + archivePath + "</archivePath>\n"
				+ "    <bucketFormatPriority>" + "SPLUNK_BUCKET"
				+ "</bucketFormatPriority>\n" + "</ns2:archiverConf>";

		List<String> archiveFormats = asList("SPLUNK_BUCKET", "CSV");
		File file = createFile();
		file.deleteOnExit();
		FileUtils.writeStringToFile(file, configFilePreset);
		archiverMBean = ShuttlArchiver.createWithConfFile(file);
		assertEquals(archiverMBean.getArchiveFormats(), archiveFormats);
		assertEquals(archiverMBean.getClusterName(), clusterName);
		assertEquals(archiverMBean.getServerName(), serverName);
		assertEquals(archiverMBean.getBackendName(), backendName);
		assertEquals(archiverMBean.getArchivePath(), archivePath);
	}

	private File getTempFile() throws Exception {
		return TUtilsMBean.createEmptyConfInNamespace("archiverConf");
	}

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void load_oldVersionWithHdfsArchiverRootURI_setsBackendAndPathWithURIAndHadoopProperties(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				try {
					TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
					setBackend_archivePath_andHadoopProperties();
				} catch (IOException e) {
					TUtilsTestNG.failForException("Got IOException", e);
				}
			}
		});
	}

	private void setBackend_archivePath_andHadoopProperties() throws IOException {
		String host = "thehost";
		String port = "9876";
		String archiverRootURI = "hdfs://" + host + ":" + port + "/archiver_root";

		createArchiverMbeanWithArchiverRootURI(archiverRootURI);
		assertEquals("hdfs", archiverMBean.getBackendName());
		assertEquals("/archiver_root", archiverMBean.getArchivePath());

		Properties hdfsProperties = new Properties();
		File hdfsPropertiesFile = BackendConfigurationFiles.create().getByName(
				HdfsProperties.HDFS_PROPERTIES_FILENAME);
		hdfsProperties.load(FileUtils.openInputStream(hdfsPropertiesFile));
		assertEquals(host, hdfsProperties.getProperty("hadoop.host"));
		assertEquals(port, hdfsProperties.getProperty("hadoop.port"));
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

	@Test(groups = { "end-to-end" })
	@Parameters(value = { "splunk.home" })
	public void load_oldVersionWithS3ArchiverRootURI_setsBackendAndPathWithURIAndAmazonProperties(
			final String splunkHome) {
		TUtilsEnvironment.runInCleanEnvironment(new Runnable() {

			@Override
			public void run() {
				try {
					TUtilsEnvironment.setEnvironmentVariable("SPLUNK_HOME", splunkHome);
					setBackend_archivePath_andAmazonProperties();
				} catch (IOException e) {
					TUtilsTestNG.failForException("Got IOException", e);
				}
			}
		});
	}

	private void setBackend_archivePath_andAmazonProperties() throws IOException {
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
}

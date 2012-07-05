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

import static java.util.Arrays.*;
import static org.testng.Assert.*;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * White box testing of MBeans
 */
@Test(groups = { "fast-unit" })
public class ShuttlArchiverMBeanTest {
	ShuttlArchiverMBean archiverMBean;

	@BeforeMethod(groups = { "fast-unit" })
	public void createMBean() throws Exception {
		File confFile = getTempFile();
		System.out.println("ShuttlArchiverMBeanTest - running "
				+ confFile.getPath());
		archiverMBean = new ShuttlArchiver(confFile.getPath());
	}

	@Test(groups = { "fast-unit" })
	public void setArchiverRoot_archiverRootIsSet_gotArchiverRoot() {
		String archiverRoot = "/test_archive_root";
		assertNotEquals(archiverMBean.getArchiverRootURI(), archiverRoot);
		archiverMBean.setArchiverRootURI(archiverRoot);
		assertEquals(archiverMBean.getArchiverRootURI(), archiverRoot);
	}

	public void setArchiveFormat_archiveFormatIsSet_gotArchiveFormat() {
		List<String> archiveFormats1 = asList("SPLUNK_BUCKET");
		assertNotEquals(archiverMBean.getArchiveFormats(), archiveFormats1);
		archiverMBean.setArchiveFormats(archiveFormats1);
		assertEquals(archiverMBean.getArchiveFormats(), archiveFormats1);
	}

	public void setClusterName_clusterNameIsSet_gotClusterName() {
		String clusterName = "test_cluster_name";
		assertNotEquals(archiverMBean.getClusterName(), clusterName);
		archiverMBean.setClusterName(clusterName);
		assertEquals(archiverMBean.getClusterName(), clusterName);
	}

	public void setTmpDirectory_directoryIsSet_gotDirectory() {
		String directoryName = "test_directory";
		assertNotEquals(archiverMBean.getTmpDirectory(), directoryName);
		archiverMBean.setTmpDirectory(directoryName);
		assertEquals(archiverMBean.getTmpDirectory(), directoryName);
	}

	public void setServerName_serverNameIsSet_gotCluserName() {
		String serverName = "test_server_name";
		assertNotEquals(archiverMBean.getServerName(), serverName);
		archiverMBean.setServerName(serverName);
		assertEquals(archiverMBean.getServerName(), serverName);
	}

	public void addIndex_indexIsSet_indexNamesContainsIndex() throws Exception {
		String index = "index";
		List<String> indexNames = archiverMBean.getIndexNames();
		assertTrue(indexNames == null || !indexNames.contains(index));
		archiverMBean.addIndex(index);
		assertTrue(archiverMBean.getIndexNames().contains(index));
	}

	public void addIndex_indexesAreSet_indexNamesContainsIndexes()
			throws Exception {
		String index1 = "index1";
		String index2 = "index2";
		List<String> indexNames = archiverMBean.getIndexNames();
		assertTrue(indexNames == null || !indexNames.contains(index1));
		assertTrue(indexNames == null || !indexNames.contains(index2));
		archiverMBean.addIndex(index1);
		archiverMBean.addIndex(index2);
		assertTrue(archiverMBean.getIndexNames().contains(index1));
		assertTrue(archiverMBean.getIndexNames().contains(index2));
	}

	public void setBucketFormatPriority_priorityIsSet_gotPriority() {
		List<String> bucketFormatPriority = Arrays.asList("SPLUNK_BUCKET",
				"UNKNOWN");
		assertNotEquals(archiverMBean.getBucketFormatPriority(),
				bucketFormatPriority);
		archiverMBean.setBucketFormatPriority(bucketFormatPriority);
		assertEquals(archiverMBean.getBucketFormatPriority(), bucketFormatPriority);
	}

	public void deleteIndex_indexIsDeleted_indexNotInIndexNames()
			throws Exception {
		String index = "index";
		archiverMBean.addIndex(index);
		archiverMBean.deleteIndex(index);
		assertFalse(archiverMBean.getIndexNames().contains(index));
		assertTrue(archiverMBean.getIndexNames().isEmpty());
	}

	public void deleteIndex_indexesAreDeleted_indexesNotInIndexNames()
			throws Exception {
		String index1 = "index1";
		String index2 = "index2";
		archiverMBean.addIndex(index1);
		archiverMBean.addIndex(index2);
		assertTrue(archiverMBean.getIndexNames().contains(index1));
		assertTrue(archiverMBean.getIndexNames().contains(index2));
		archiverMBean.deleteIndex(index1);
		archiverMBean.deleteIndex(index2);
		assertFalse(archiverMBean.getIndexNames().contains(index1));
		assertFalse(archiverMBean.getIndexNames().contains(index2));
		assertTrue(archiverMBean.getIndexNames().isEmpty());
	}

	public void save_configured_producesCorrectXML() throws Exception {
		List<String> archiveFormats = asList("SPLUNK_BUCKET", "CSV");
		String clusterName = "some_cluster_name";
		String serverName = "some_server_name";
		String archiverRootURI = "hdfs://localhost:1234";
		String tmpDirectory = "/some-tmp-dir";
		String expectedConfigFile = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "<archiveFormats>\n"
				+ "<archiveFormat>SPLUNK_BUCKET</archiveFormat>\n"
				+ "<archiveFormat>CSV</archiveFormat>\n"
				+ "</archiveFormats>\n"
				+ "<clusterName>"
				+ clusterName
				+ "</clusterName>\n"
				+ "<serverName>"
				+ serverName
				+ "</serverName>\n"
				+ "<archiverRootURI>"
				+ archiverRootURI
				+ "</archiverRootURI>\n"
				+ "<bucketFormatPriority>"
				+ "SPLUNK_BUCKET"
				+ "</bucketFormatPriority>\n"
				+ "<bucketFormatPriority>"
				+ "CSV"
				+ "</bucketFormatPriority>\n"
				+ "<tmpDirectory>"
				+ tmpDirectory
				+ "</tmpDirectory>\n"
				+ "</ns2:archiverConf>\n";

		File file = getTempFile();
		archiverMBean = new ShuttlArchiver(file.getPath());
		archiverMBean.setArchiveFormats(archiveFormats);
		archiverMBean.setClusterName(clusterName);
		archiverMBean.setServerName(serverName);
		archiverMBean.setArchiverRootURI(archiverRootURI);
		archiverMBean.setTmpDirectory(tmpDirectory);
		archiverMBean.setBucketFormatPriority(archiveFormats);
		archiverMBean.save();

		assertEquals(noSpaces(FileUtils.readFileToString(file)),
				noSpaces(expectedConfigFile));
	}

	private String noSpaces(String s) {
		return s.replaceAll(" ", "");
	}

	public void load_preconfiguredFile_givesCorrectValues() throws Exception {
		List<String> archiveFormats = asList("SPLUNK_BUCKET", "CSV");
		String clusterName = "some_cluster_name";
		String serverName = "some_server_name";
		String archiverRootURI = "hdfs://localhost:1234";
		String tmpDirectory = "/some-tmp-dir";
		String configFilePreset = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">\n"
				+ "<archiveFormats>\n"
				+ "<archiveFormat>SPLUNK_BUCKET</archiveFormat>\n"
				+ "<archiveFormat>CSV</archiveFormat>\n"
				+ "</archiveFormats>\n"
				+ "<clusterName>"
				+ clusterName
				+ "</clusterName>\n"
				+ "    <serverName>"
				+ serverName
				+ "</serverName>\n"
				+ "    <archiverRootURI>"
				+ archiverRootURI
				+ "</archiverRootURI>\n"
				+ "    <bucketFormatPriority>"
				+ "SPLUNK_BUCKET"
				+ "</bucketFormatPriority>\n"
				+ "    <tmpDirectory>"
				+ tmpDirectory + "</tmpDirectory>\n" + "</ns2:archiverConf>";

		File file = File.createTempFile("shuttlArchiverMBeanTest2", ".xml");
		file.deleteOnExit();
		FileUtils.writeStringToFile(file, configFilePreset);
		archiverMBean = new ShuttlArchiver(file.getPath());
		assertEquals(archiverMBean.getArchiveFormats(), archiveFormats);
		assertEquals(archiverMBean.getClusterName(), clusterName);
		assertEquals(archiverMBean.getServerName(), serverName);
		assertEquals(archiverMBean.getArchiverRootURI(), archiverRootURI);
		assertEquals(archiverMBean.getTmpDirectory(), tmpDirectory);
	}

	private File getTempFile() throws Exception {
		File confFile = File.createTempFile("shuttlArchiverMBeanTest", ".xml");
		String emptyConfigFile = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
				+ "<ns2:archiverConf xmlns:ns2=\"com.splunk.shuttl.server.model\">"
				+ "</ns2:archiverConf>";
		confFile.deleteOnExit();
		FileUtils.writeStringToFile(confFile, emptyConfigFile);
		return confFile;
	}

}

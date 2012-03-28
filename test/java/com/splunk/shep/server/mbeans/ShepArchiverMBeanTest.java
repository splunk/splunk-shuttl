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
import java.io.FileWriter;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * White box testing of MBeans
 * 
 * @author kpakkirisamy
 * 
 */
public class ShepArchiverMBeanTest {
    private static final String EMPTY_XML = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
	    + "<ns2:archiverConf xmlns:ns2=\"com.splunk.shep.server.model\"></ns2:archiverConf>";
    private static final String TEST_ARCHIVE_ROOT = "/test_archive_root";
    private static final String TEST_CLUSTER_NAME = "test_cluster_name";
    private static final String TEST_INDEX1 = "test_index1";
    private static final String TEST_INDEX2 = "test_index2";

    private ShepArchiver archiverMBean = null;

    @BeforeClass(groups = { "fast-unit" })
    public void createMBean() throws Exception {
	try {
	    File confFile = getTempFile();
	    System.out.println("ShepArchiverMBeanTest - running "
		    + confFile.getPath());
	    this.archiverMBean = new ShepArchiver(confFile.getPath());
	    this.archiverMBean.setArchiverRoot(TEST_ARCHIVE_ROOT);
	    this.archiverMBean.setClusterName(TEST_CLUSTER_NAME);
	    this.archiverMBean.save();
	    this.archiverMBean.refresh();
	} catch (Exception e) {
	    e.printStackTrace();
	    throw new Exception(e);
	}
    }

    @Test(groups = { "fast-unit" })
    public void test_archiver_root() {
	String archiverRoot = this.archiverMBean.getArchiverRoot();
	assertEquals(archiverRoot, TEST_ARCHIVE_ROOT);
    }

    @Test(groups = { "fast-unit" })
    public void test_clustername() {
	String clustername = this.archiverMBean.getClusterName();
	assertEquals(clustername, TEST_CLUSTER_NAME);
    }

    @Test(groups = { "fast-unit" })
    public void test_addIndex() throws ShepMBeanException {
	addIndexes();
	java.util.List<String> indexNames = this.archiverMBean.getIndexNames();
	boolean found1 = false;
	boolean found2 = false;
	for (String index : indexNames) {
	    if (index.equals(TEST_INDEX1)) {
		found1 = true;
	    }
	    if (index.equals(TEST_INDEX2)) {
		found2 = true;
	    }
	}
	assert (found1 && found2); // both index names should be there
	deleteIndexes();
    }

    @Test(groups = { "fast-unit" })
    public void test_deleteIndex() throws ShepMBeanException {
	addIndexes();
	deleteIndexes();
	java.util.List<String> indexNames = this.archiverMBean.getIndexNames();
	boolean found1 = false;
	boolean found2 = false;
	for (String index : indexNames) {
	    if (index.equals(TEST_INDEX1)) {
		found1 = true;
	    }
	    if (index.equals(TEST_INDEX2)) {
		found2 = true;
	    }
	}
	assert (!found1 && !found2); // both index names should NOT be there
    }

    private void addIndexes() throws ShepMBeanException {
	this.archiverMBean.addIndex(TEST_INDEX1);
	this.archiverMBean.addIndex(TEST_INDEX2);
	this.archiverMBean.save();
	this.archiverMBean.refresh();
    }

    private void deleteIndexes() throws ShepMBeanException {
	this.archiverMBean.deleteIndex(TEST_INDEX1);
	this.archiverMBean.deleteIndex(TEST_INDEX2);
	this.archiverMBean.save();
	this.archiverMBean.refresh();
    }

    private File getTempFile() throws Exception {
	File confFile = File.createTempFile("shepArchiverMBeanTest", ".xml");
	confFile.deleteOnExit();
	FileWriter writer = new FileWriter(confFile);
	writer.write(EMPTY_XML);
	writer.close();
	return confFile;
    }

}

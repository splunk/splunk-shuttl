package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class ArchiveConfigurationTest {

    ArchiveConfiguration configuration;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	configuration = new ArchiveConfiguration();
    }

    @Test(groups = { "fast" })
    public void getArchiveFormat_defaultState_isNotNull() {
	assertNotNull(configuration.getArchiveFormat());
    }

    public void getArchivingRoot_defaultState_isNotNull() {
	assertNotNull(configuration.getArchivingRoot());
    }

    public void getClusterName_defaultState_isNotNull() {
	assertNotNull(configuration.getClusterName());
    }

    public void getServerName_defaultState_isNotNull() {
	assertNotNull(configuration.getServerName());
    }

    public void getTmpDirectory_defaultState_isNotNull() {
	assertNotNull(configuration.getTmpDirectory());
    }

    public void getTmpDirectory_defaultState_defaultConfValue() {
	assertNotNull(configuration.getTmpDirectory());
    }

    public void getArchiverHadoopURI_defaultState_isNotNull() {
	assertNotNull(configuration.getArchiverHadoopURI());
    }

    public void getArchiverHadoopURI_defaultState_defaultValue() {
	assertEquals(new Path("hdfs://localhost:9000/archiver-tmp"),
		configuration.getTmpDirectory());
    }

}

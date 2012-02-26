package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class ArchiveConfigurationTest {

    public void getArchiveFormat_defaultState_isNotNull() {
	ArchiveConfiguration config = new ArchiveConfiguration();
	assertNotNull(config.getArchiveFormat());
    }
}

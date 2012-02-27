package com.splunk.shep.mapred.lib.rest;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

public class MapRedRestTestConstants {

    public static final String TEST_RESOURCES_PATH = "test/java/com/splunk/shep/mapred/lib/rest";

    @Test(groups = { "fast" })
    public void testResourcesPathConstant_should_equal_theFolderOfThePackageWhereThisClassLives() {
	assertEquals(TEST_RESOURCES_PATH,
		"test/java/com/splunk/shep/mapred/lib/rest");
    }
}

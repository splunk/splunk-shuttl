package com.splunk.shep.testutil;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class SafePathCreatorTest {

    SafePathCreator safePathCreator;

    @BeforeTest(groups = { "fast" })
    public void setUp() {
	safePathCreator = SafePathCreator.get();
    }

    @Test(groups = { "fast" })
    public void safePath_should_beSeparated_by_HomeDirectoryAndNameOfTestCase_toAchieve_nicerStructure() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	Path safePath = safePathCreator.getPathOnFileSystem(fileSystem);
	Path expected = new Path(fileSystem.getHomeDirectory() + "/"
		+ this.getClass().getName());
	assertEquals(safePath, expected);
    }
}

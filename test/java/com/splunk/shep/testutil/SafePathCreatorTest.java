package com.splunk.shep.testutil;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SafePathCreatorTest {

    SafePathCreator safePathCreator;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	safePathCreator = SafePathCreator.create();
    }

    @Test(groups = { "fast" })
    public void safePath_should_beSeparated_by_HomeDirectoryAndNameOfTestCase_toAchieve_nicerStructure() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	Path safePath = safePathCreator.getSafeDirectory(fileSystem,
		this.getClass());
	Path expected = new Path(fileSystem.getHomeDirectory() + "/"
		+ this.getClass().getName());
	assertEquals(safePath, expected);
    }
}

package com.splunk.shep.testutil;

import static org.testng.Assert.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class UtilsPathTest {

    @Test(groups = { "fast-unit" })
    public void safePath_should_beSeparated_by_HomeDirectoryAndNameOfTestCase_toAchieve_nicerStructure() {
	FileSystem fileSystem = UtilsFileSystem.getLocalFileSystem();
	Path safePath = UtilsPath.getSafeDirectory(fileSystem);
	Path expected = new Path(fileSystem.getHomeDirectory() + "/"
		+ this.getClass().getName());
	assertEquals(safePath, expected);
    }
}

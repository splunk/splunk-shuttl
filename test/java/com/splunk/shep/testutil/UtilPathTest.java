package com.splunk.shep.testutil;

import static org.testng.Assert.*;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

public class UtilPathTest {

    @Test(groups = { "fast" })
    public void safePath_should_beSeparated_by_HomeDirectoryAndNameOfTestCase_toAchieve_nicerStructure() {
	FileSystem fileSystem = FileSystemUtils.getLocalFileSystem();
	Path safePath = UtilPath.getSafeDirectory(fileSystem);
	Path expected = new Path(fileSystem.getHomeDirectory() + "/"
		+ this.getClass().getName());
	assertEquals(safePath, expected);
    }
}

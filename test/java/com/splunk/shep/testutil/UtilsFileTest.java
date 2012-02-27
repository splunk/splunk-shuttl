package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class UtilsFileTest {

    public void createTempDirectory_tenTwo_uniqueAndNotNull() {
	int times = 2;
	Set<String> absolutePaths = new HashSet<String>();
	for (int i = 0; i < times; i++) {
	    String absolutePath = UtilsFile.createTempDirectory()
		    .getAbsolutePath();
	    assertNotNull(absolutePath);
	    absolutePaths.add(absolutePath);
	}
	assertEquals(times, absolutePaths.size());
    }

    public void createTempDirectory_containNameOfThisTestClass_whenCalled() {
	File tempDir = UtilsFile.createTempDirectory();
	String dirName = tempDir.getName();
	assertTrue(dirName.contains(getClass().getSimpleName()));
    }

    public void createNamedTempDirectory_fileDoesNotExist_getsCreated() {
	File dir = UtilsFile.createNamedTempDirectory("NameOfTheDirectory");
	assertTrue(dir.exists());
    }

    @Test(expectedExceptions = { RuntimeException.class })
    public void createNamedTempDirectory_twice_throwsRuntimeException() {
	String name = "someName";
	UtilsFile.createNamedTempDirectory(name);
	UtilsFile.createNamedTempDirectory(name);
    }
}

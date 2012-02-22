package com.splunk.shep.archiver.fileSystem;

import static org.testng.AssertJUnit.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class FileSystemPathTest {

    private FileSystemPath fileSystemPath;
    public static final String TEST_PATH_AS_STRING = "/This/is/a/valid/path";

    @BeforeMethod
    public void beforeMethod() {
	fileSystemPath = new FileSystemPath(TEST_PATH_AS_STRING);

    }

    public void FileSystemPath() {
	assertNotNull(fileSystemPath);
    }

    public void getPathAsString() {
	assertEquals(TEST_PATH_AS_STRING, fileSystemPath.getPathAsString());
    }

    // Had to name it with Test suffix, otherwise this overrides the objects
    // toString method.
    public void toStringTest() {
	assertEquals(TEST_PATH_AS_STRING, fileSystemPath.toString());
    }
}

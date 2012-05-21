package com.splunk.shuttl.archiver.fileSystem;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.fileSystem.FileOverwriteException;

@Test(groups = { "fast-unit" })
public class FileOverwriteExceptionTest {
    
    @Test(groups = { "fast-unit" })
    public void fileOverwriteExceptionTest() {
	AssertJUnit.assertNotNull(new FileOverwriteException());
  }
}

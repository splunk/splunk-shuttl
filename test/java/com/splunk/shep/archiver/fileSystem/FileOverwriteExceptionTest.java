package com.splunk.shep.archiver.fileSystem;

import org.testng.AssertJUnit;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class FileOverwriteExceptionTest {
    
    @Test(groups = { "fast" })
    public void fileOverwriteExceptionTest() {
	AssertJUnit.assertNotNull(new FileOverwriteException());
  }
}

package com.splunk.shep.testutil;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.testng.AssertJUnit;
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
	File dir = UtilsFile.createPrefixedTempDirectory("NameOfTheDirectory");
	assertTrue(dir.exists());
    }

    public void createNamedTempDirectory_containsNameOfThisClass_toProvideUniquenessToTheDirectory() {
	File dir = UtilsFile.createPrefixedTempDirectory("someName");
	String dirName = dir.getName();
	assertTrue(dirName.contains(getClass().getSimpleName()));
    }

    public void createNamedTempDirectory_withFileAsParentParameter_createsTheDirectoryInParent()
	    throws IOException {
	File parent = UtilsFile.createPrefixedTempDirectory("parent");
	File child = UtilsFile.createDirectoryInParent(parent, "child");
	assertEquals(parent.listFiles()[0], child);
	File childsChild = UtilsFile.createDirectoryInParent(child,
		"childsChild");
	assertEquals(child.listFiles()[0], childsChild);

	// Teardown
	FileUtils.deleteDirectory(parent);
	assertTrue(!childsChild.exists());
	assertTrue(!child.exists());
	assertTrue(!parent.exists());
    }

    public void createTestFileWithContentsOfFile_validInput_diffrentPaths() {
	File file = UtilsFile.createTestFileWithRandomContent();
	File newFile = UtilsFile.createTestFileWithContentsOfFile(file);
	AssertJUnit.assertFalse(file.getPath().equals(newFile.getPath()));
    }

    public void createTestFileWithContentsOfFile_validInput_sameContent() {
	File file = UtilsFile.createTestFileWithRandomContent();
	File newFile = UtilsFile.createTestFileWithContentsOfFile(file);
	UtilsTestNG.assertFileContentsEqual(file, newFile);
    }

    public void createFileInParent_givenNameOfFile_createFileInParent()
	    throws IOException {
	String childFileName = "child";
	File parent = UtilsFile.createTempDirectory();
	File child = UtilsFile.createFileInParent(parent, childFileName);
	assertEquals(parent, child.getParentFile());
	assertEquals(childFileName, child.getName());

	// Teardown
	FileUtils.deleteDirectory(parent);
    }

    public void createTmpDirectoryWithName_givenAName_createAnExistingDirectoryWithSPecifiedName() {
	File file = UtilsFile
		.createTmpDirectoryWithName("this is the name of the file");

	assertTrue(file.exists());
	assertTrue(file.isDirectory());
	assertEquals("this is the name of the file", file.getName());
    }
}

package com.splunk.shuttl.archiver.util;

import static org.testng.AssertJUnit.*;

import org.apache.hadoop.fs.Path;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.util.UtilsPath;

@Test(groups = { "fast-unit" })
public class UtilsPathTest {
  public void createPathByAppending_pathsWithSchemes_theNewPathShouldProperlyBeCreated() {
      Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp");
      Path pathToAppend = new Path("hdfs://localhost:9000/an/other/path");
      
      // Test
      Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended, pathToAppend);
      
      // Verify
      assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path"), newPath);
  }

    @Test
    public void createPathByAppending_pathsWithSchemesAndEndingSlahs_theNewPathShouldProperlyBeCreated() {
	Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
	Path pathToAppend = new Path("hdfs://localhost:9000/an/other/path/");

	// Test
	Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
		pathToAppend);

	// Verify
	assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"),
		newPath);
    }

    @Test
    public void createPathByAppending_appendPathHavingDiffrentScheme_schemeOfTheToBeAppededPathShouldBeUsed() {
	Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
	Path pathToAppend = new Path("file://localhost:9000/an/other/path/");

	// Test
	Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
		pathToAppend);

	// Verify
	assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"),
		newPath);
    }

    @Test
    public void createPathByAppending_appendPathWithOutScheme_schemeOfTheToBeAppededPathShouldBeUsed() {
	Path pathThatWillBeAppended = new Path("hdfs://localhost:9000/tmp/");
	Path pathToAppend = new Path("/an/other/path/");

	// Test
	Path newPath = UtilsPath.createPathByAppending(pathThatWillBeAppended,
		pathToAppend);

	// Verify
	assertEquals(new Path("hdfs://localhost:9000/tmp/an/other/path/"),
		newPath);
    }
}

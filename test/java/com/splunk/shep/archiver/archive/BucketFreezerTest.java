package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.ShellClassRunner;

@Test(groups = { "fast" })
public class BucketFreezerTest {

    private ShellClassRunner shellClassRunner;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	shellClassRunner = new ShellClassRunner();
    }

    @AfterMethod(groups = { "fast" })
    public void tearDown() {
	deleteDirectory(getTestDirectory());
    }

    private void deleteDirectory(File dir) {
	try {
	    FileUtils.deleteDirectory(dir);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    @Test
    public void testDirectory_shouldNot_exist() {
	assertFalse(getTestDirectory().exists());
    }

    @Test
    public void should_returnExitStatusZero_when_runWithOneArgument_where_theArgumentIsAnExistingDirectory() {
	File directory = createTestDirectory();
	assertEquals(0, runBucketFreezerMain(directory.getAbsolutePath()));
    }

    @Test
    public void should_returnExitStatus_1_when_runWithZeroArguments() {
	assertEquals(1, runBucketFreezerMain());
    }

    @Test
    public void should_returnExitStatus_2_when_runWithMoreThanOneArgument() {
	assertEquals(2, runBucketFreezerMain("one", "two"));
    }

    private int runBucketFreezerMain(String... args) {
	return shellClassRunner.runClassWithArgs(BucketFreezer.class, args)
		.getExitCode();
    }

    @Test
    public void should_returnExitStatus_3_when_runWithArgumentThatIsNotADirectory()
	    throws IOException {
	File file = File.createTempFile("ArchiveTest", ".tmp");
	file.deleteOnExit();
	assertTrue(!file.isDirectory());
	assertEquals(3, runBucketFreezerMain(file.getAbsolutePath()));
    }

    @Test
    public void should_moveDirectory_to_aSafeLocation_when_givenPath() {
	String safeLocation = System.getProperty("user.home") + "/"
		+ getClass().getName();
	File safeLocationDirectory = new File(safeLocation);
	File dirToBeMoved = createTestDirectory();

	BucketFreezer bucketFreezer = new BucketFreezer(safeLocation);
	bucketFreezer.freezeBucket(dirToBeMoved.getAbsolutePath());
	assertEquals(0, bucketFreezer.getExitStatus());

	assertTrue(!dirToBeMoved.exists());
	File dirInSafeLocation = new File(safeLocationDirectory,
		dirToBeMoved.getName());
	assertTrue(dirInSafeLocation.exists());
	assertTrue(dirInSafeLocation.delete());
	assertTrue(safeLocationDirectory.exists());
    }

    private File getTestDirectory() {
	return new File(getClass().getSimpleName() + "-test-dir");
    }

    private File createTestDirectory() {
	return createDirectory(getTestDirectory());
    }

    private File createDirectory(File dir) {
	dir.mkdir();
	assertTrue(dir.exists());
	return dir;
    }

}

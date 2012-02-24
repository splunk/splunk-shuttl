package com.splunk.shep.archiver.archive;

import static org.testng.AssertJUnit.*;

import java.io.File;
import java.io.IOException;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.splunk.shep.testutil.ShellClassRunner;

@Test(groups = { "fast" })
public class ArchiveBucketTest {

    private ShellClassRunner shellClassRunner;

    @BeforeMethod(groups = { "fast" })
    public void setUp() {
	shellClassRunner = new ShellClassRunner();
    }

    public void should_returnExitStatusZero_when_runWithOneArgument_where_theArgumentIsAnExistingDirectory() {
	File directory = getDirectory();
	assertEquals(0, runArchiveBucketMain(directory.getAbsolutePath()));
    }

    public void should_returnExitStatus_1_when_runWithZeroArguments() {
	assertEquals(1, runArchiveBucketMain());
    }

    public void should_returnExitStatus_2_when_runWithMoreThanOneArgument() {
	assertEquals(2, runArchiveBucketMain("one", "two"));
    }

    private int runArchiveBucketMain(String... args) {
	return shellClassRunner.runClassWithArgs(ArchiveBucket.class, args)
		.getExitCode();
    }

    public void should_returnExitStatus_3_when_runWithArgumentThatIsNotADirectory()
	    throws IOException {
	File file = File.createTempFile("ArchiveTest", ".tmp");
	file.deleteOnExit();
	assertTrue(!file.isDirectory());
	assertEquals(3, runArchiveBucketMain(file.getAbsolutePath()));
    }

    @Test
    public void should_moveDirectory_to_aSafeLocation_when_givenPath() {
	File dir = getDirectory();

	assertEquals(0, runArchiveBucketMain(dir.getAbsolutePath()));
	assertTrue(!dir.exists());

	File dirInSafeLocation = new File(ArchiveBucket.SAFE_LOCATION,
		dir.getName());
	assertTrue(dirInSafeLocation.exists());
	assertTrue(dirInSafeLocation.delete());
    }

    private File getDirectory() {
	File dir = new File("dir");
	dir.mkdir();
	dir.deleteOnExit(); // In case something goes wrong, delete on exit.
	assertTrue(dir.exists());
	return dir;
    }

}

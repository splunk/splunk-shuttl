package com.splunk.shep.archiver.fileSystem;

import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.*;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Test(groups = { "fast" })
public class FileStatusBackedListTest {

    private FileStatusBackedList uriList;
    private FileStatus[] fileStatus;

    @BeforeMethod
    public void beforeMethod() {
	fileStatus = new FileStatus[] { mock(FileStatus.class),
		mock(FileStatus.class) };
	uriList = new FileStatusBackedList(fileStatus);
    }

    public void FileStatusBackedList() {
	assertNotNull(uriList);
    }

    public void get_correctIndex_correctItem() throws URISyntaxException {
	URI uri0 = new URI("file:///path/to/a/file");
	URI uri1 = new URI("file:///path/to/an/other/file");
	when(fileStatus[0].getPath()).thenReturn(new Path(uri0));
	when(fileStatus[1].getPath()).thenReturn(new Path(uri1));
	
	// Test
	assertEquals(uri0, uriList.get(0));
	assertEquals(uri1, uriList.get(1));
	
	
    }

    public void size_emptyList_returnZero() {
	assertEquals(0, (new FileStatusBackedList(new FileStatus[0])).size());
    }

    public void size_nonEmpty_returnCorrectSize() {
	assertEquals(2, uriList.size());
    }
}

package com.splunk.shep.testutil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalFileToHadoopCopier {

    private final FileSystem fileSystem;

    public static class LocalFileNotFound extends RuntimeException {
    }

    public LocalFileToHadoopCopier(FileSystem fileSystem) {
	this.fileSystem = fileSystem;
    }

    public void copyFileToHadoop(File source, Path destination) {
	if (!source.exists())
	    throw new LocalFileNotFound();
	else
	    doCopyFileToHadoop(source, destination);
    }

    private void doCopyFileToHadoop(File src, Path dst) {
	try {
	    fileSystem.copyFromLocalFile(new Path(src.getPath()), dst);
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }
}

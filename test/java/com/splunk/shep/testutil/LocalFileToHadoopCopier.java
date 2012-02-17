package com.splunk.shep.testutil;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class LocalFileToHadoopCopier {

    public static class LocalFileNotFound extends RuntimeException {
	private static final long serialVersionUID = 1L;
    }

    private final FileSystem fileSystem;
    private final SafePathCreator safePathCreator;

    public LocalFileToHadoopCopier(FileSystem fileSystem,
	    SafePathCreator safePathCreator) {
	this.fileSystem = fileSystem;
	this.safePathCreator = safePathCreator;
    }

    public void copyFileToHadoop(File source) {
	if (!source.exists())
	    throw new LocalFileNotFound();
	else
	    doCopyFileToHadoop(source);
    }

    private void doCopyFileToHadoop(File src) {
	try {
	    fileSystem.copyFromLocalFile(new Path(src.getPath()),
		    getSafePathOnFileSystemForFile(src));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    private Path getSafePathOnFileSystemForFile(File src) {
	Path safeDirectory = safePathCreator.getPathOnFileSystem(fileSystem);
	return new Path(safeDirectory, src.getName());
    }

    public boolean isFileCopiedToFileSystem(File file) {
	try {
	    return fileSystem.exists(getSafePathOnFileSystemForFile(file));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    public static LocalFileToHadoopCopier get(FileSystem fileSystem) {
	return new LocalFileToHadoopCopier(fileSystem, SafePathCreator.get());
    }
}

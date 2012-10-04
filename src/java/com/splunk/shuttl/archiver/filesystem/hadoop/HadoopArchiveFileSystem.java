// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// License); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an AS IS BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.splunk.shuttl.archiver.filesystem.hadoop;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionalFileSystem;
import com.splunk.shuttl.archiver.model.Bucket;

public class HadoopArchiveFileSystem implements ArchiveFileSystem,
		TransactionalFileSystem {

	private final FileSystem hadoopFileSystem;

	public HadoopArchiveFileSystem(FileSystem hadoopFileSystem) {
		this.hadoopFileSystem = hadoopFileSystem;
	}

	@Override
	public List<URI> listPath(URI pathToBeListed) throws IOException {
		Path hadoopPath = new Path(pathToBeListed);
		FileStatus[] fileStatusOfPath = hadoopFileSystem.listStatus(hadoopPath);
		if (fileStatusOfPath != null)
			return new FileStatusBackedList(fileStatusOfPath);
		else
			return Collections.emptyList();
	}

	@Override
	public InputStream openFile(URI fileOnArchiveFileSystem) throws IOException {
		return hadoopFileSystem.open(new Path(fileOnArchiveFileSystem));
	}

	@Override
	public void putBucket(Bucket bucket, URI temp, URI dst) throws IOException {
		putFile(bucket.getDirectory(), temp, dst);
	}

	@Override
	public void getBucket(Bucket bucket, File temp, File dst) throws IOException {
		getFile(bucket.getURI(), temp, dst);
	}

	@Override
	public void putFile(File src, URI temp, URI dst) throws IOException {
		if (hadoopFileSystem.exists(new Path(dst)))
			throw new FileOverwriteException();
		Path tempPath = new Path(temp);
		hadoopFileSystem.delete(tempPath, true);
		hadoopFileSystem.copyFromLocalFile(new Path(src.toURI()), tempPath);
	}

	@Override
	public void getFile(URI src, File temp, File dst) throws IOException {
		if (dst.exists())
			throw new FileOverwriteException();
		hadoopFileSystem.copyToLocalFile(new Path(src), new Path(temp.toURI()));
	}

	@Override
	public void mkdirs(URI uri) throws IOException {
		mkdirsWithPath(new Path(uri));
	}

	private void mkdirsWithPath(Path path) throws IOException {
		hadoopFileSystem.mkdirs(path);
	}

	@Override
	public void rename(URI from, URI to) throws IOException {
		mkdirsWithPath(new Path(to).getParent());
		hadoopFileSystem.rename(new Path(from), new Path(to));
	}

	@Override
	public void cleanFileTransaction(URI src, URI temp) {
		// do nothing.
	}

	@Override
	public void cleanBucketTransaction(Bucket bucket, URI temp) {
		// do nothing.
	}

}

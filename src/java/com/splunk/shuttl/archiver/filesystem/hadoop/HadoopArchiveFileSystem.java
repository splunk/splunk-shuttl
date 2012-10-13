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
import java.util.Collections;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splunk.shuttl.archiver.filesystem.ArchiveFileSystem;
import com.splunk.shuttl.archiver.filesystem.FileOverwriteException;
import com.splunk.shuttl.archiver.filesystem.transaction.TransactionalFileSystem;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.BucketTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.bucket.TransfersBuckets;
import com.splunk.shuttl.archiver.filesystem.transaction.file.FileTransactionCleaner;
import com.splunk.shuttl.archiver.filesystem.transaction.file.TransfersFiles;
import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.LocalBucket;

public class HadoopArchiveFileSystem implements ArchiveFileSystem,
		TransactionalFileSystem {

	private final FileSystem hadoopFileSystem;

	public HadoopArchiveFileSystem(FileSystem hadoopFileSystem) {
		this.hadoopFileSystem = hadoopFileSystem;
	}

	@Override
	public List<String> listPath(String pathToBeListed) throws IOException {
		Path hadoopPath = new Path(pathToBeListed);
		FileStatus[] fileStatusOfPath = hadoopFileSystem.listStatus(hadoopPath);
		if (fileStatusOfPath != null)
			return new FileStatusBackedList(fileStatusOfPath);
		else
			return Collections.emptyList();
	}

	@Override
	public InputStream openFile(String fileOnArchiveFileSystem)
			throws IOException {
		return hadoopFileSystem.open(new Path(fileOnArchiveFileSystem));
	}

	private void putFile(File src, Path temp, Path dst) throws IOException {
		if (hadoopFileSystem.exists(dst))
			throw new FileOverwriteException();
		hadoopFileSystem.delete(temp, true);
		hadoopFileSystem.copyFromLocalFile(new Path(src.toURI()), temp);
	}

	private void getFile(Path src, File temp, File dst) throws IOException {
		if (dst.exists())
			throw new FileOverwriteException();
		FileUtils.deleteDirectory(temp);
		hadoopFileSystem.copyToLocalFile(src, new Path(temp.toURI()));
	}

	@Override
	public void mkdirs(String path) throws IOException {
		mkdirsWithPath(new Path(path));
	}

	private void mkdirsWithPath(Path path) throws IOException {
		hadoopFileSystem.mkdirs(path);
	}

	@Override
	public void rename(String from, String to) throws IOException {
		mkdirsWithPath(new Path(to).getParent());
		hadoopFileSystem.rename(new Path(from), new Path(to));
	}

	@Override
	public TransfersBuckets getBucketTransferer() {
		return new TransfersBuckets() {

			@Override
			public void put(Bucket bucket, String temp, String dst)
					throws IOException {
				LocalBucket localBucket = (LocalBucket) bucket;
				putFile(localBucket.getDirectory(), new Path(temp), new Path(dst));
			}

			@Override
			public void get(Bucket remoteBucket, File temp, File dst)
					throws IOException {
				getFile(new Path(remoteBucket.getPath()), temp, dst);
			}
		};
	}

	@Override
	public TransfersFiles getFileTransferer() {
		return new TransfersFiles() {

			@Override
			public void put(String localData, String temp, String dst)
					throws IOException {
				putFile(new File(localData), new Path(temp), new Path(dst));
			}

			@Override
			public void get(String remoteData, File temp, File dst)
					throws IOException {
				getFile(new Path(remoteData), temp, dst);
			}
		};
	}

	@Override
	public BucketTransactionCleaner getBucketTransactionCleaner() {
		return new BucketTransactionCleaner() {

			@Override
			public void cleanTransaction(Bucket bucket, String temp) {
				// Do nothing.
			}
		};
	}

	@Override
	public FileTransactionCleaner getFileTransactionCleaner() {
		return new FileTransactionCleaner() {

			@Override
			public void cleanTransaction(String file, String temp) {
				// Do nothing.
			}
		};
	}

	public FileSystem getFileSystem() {
		return hadoopFileSystem;
	}
}

// Copyright (C) 2011 Splunk Inc.
//
// Splunk Inc. licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.splunk.shuttl.prototype.symlink;

import static org.testng.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.splunk.shuttl.archiver.model.Bucket;
import com.splunk.shuttl.archiver.model.IllegalIndexException;
import com.splunk.shuttl.archiver.thaw.SplunkSettings;
import com.splunk.shuttl.testutil.SplunkServiceParameters;
import com.splunk.shuttl.testutil.TUtilsBucket;
import com.splunk.shuttl.testutil.TUtilsFile;
import com.splunk.shuttl.testutil.TUtilsFunctional;

/**
 * Using results from {@link HadoopFileLocationPrototypeTest}, it should be
 * possible to symlink all the files in a bucket to a Splunk index's thaw
 * directory. It should then be possible to search for the data in Splunk.
 */
public class BucketBlockSymlinkPrototypeTest {

	Path testDataPath;
	FileSystem hadoopFileSystem;
	private Bucket realBucket;
	private File thawLocation;

	@BeforeMethod
	public void setUp() {
		realBucket = TUtilsBucket.createRealBucket();
		testDataPath = new Path("/testData");
	}

	@AfterMethod
	public void tearDown() throws IOException {
		if (hadoopFileSystem != null)
			hadoopFileSystem.delete(testDataPath, true);
		cleanThawDirectory();
	}

	private void cleanThawDirectory() {
		if (thawLocation != null)
			if (thawLocation.listFiles() != null)
				for (File f : thawLocation.listFiles())
					FileUtils.deleteQuietly(f);
	}

	@Test(groups = { "prototype" })
	@Parameters(value = { "hadoop.host", "hadoop.port", "splunk.username",
			"splunk.password", "splunk.host", "splunk.mgmtport" })
	public void test(String hadoopHost, String hadoopPort, String splunkUsername,
			String splunkPassword, String splunkHost, String splunkPort)
			throws IOException {
		Path bucketPathOnHadoop = copyRealBucketToHadoop(hadoopHost, hadoopPort);
		thawLocation = getShuttlThawLocation(splunkUsername, splunkPassword,
				splunkHost, splunkPort);
		assertTrue(TUtilsFile.isDirectoryEmpty(thawLocation));
		File thawBucket = new File(thawLocation, realBucket.getName());
		assertTrue(thawBucket.mkdirs());

		FileStatus[] pathsInBucketOnHadoop = hadoopFileSystem
				.listStatus(bucketPathOnHadoop);
		for (FileStatus fs : pathsInBucketOnHadoop)
			if (fs.isDir())
				handleRawdataDirectory(fs, thawBucket);
			else
				createSymlinkToPathInDir(fs.getPath(), thawBucket);
	}

	private void handleRawdataDirectory(FileStatus fs, File thawBucket)
			throws IOException {
		Path rawdataOnHadoop = fs.getPath();
		String rawdataName = rawdataOnHadoop.getName();
		assertEquals("rawdata", rawdataName);
		File rawdataInThaw = new File(thawBucket, rawdataName);
		assertTrue(rawdataInThaw.mkdirs()); // Create the rawdata directory
		FileStatus[] lsInRawdata = hadoopFileSystem.listStatus(fs.getPath());
		for (FileStatus fs2 : lsInRawdata) {
			if (fs2.isDir())
				throw new IllegalStateException("Cannot be another "
						+ "dir in the rawdata dir");
			else
				createSymlinkToPathInDir(fs2.getPath(), rawdataInThaw);
		}
	}

	private void createSymlinkToPathInDir(Path path, File dir) throws IOException {
		File fileInDir = new File(dir, path.getName());

		DistributedFileSystem dfs = (DistributedFileSystem) hadoopFileSystem;
		ClientProtocol namenode = dfs.getClient().namenode;
		String pathOnHadoop = path.toUri().getPath();
		LocatedBlocks blockLocations = namenode.getBlockLocations(pathOnHadoop, 0,
				Long.MAX_VALUE);
		List<LocatedBlock> locatedBlocks = blockLocations.getLocatedBlocks();
		if (!locatedBlocks.isEmpty()) {
			doSymlinkPathInDir(fileInDir, blockLocations, locatedBlocks);
		} else {
			// Means that they don't have a block and that they are empty files. Just
			// create them.
			assertTrue(fileInDir.createNewFile());
		}
	}

	private void doSymlinkPathInDir(File fileInDir, LocatedBlocks blockLocations,
			List<LocatedBlock> locatedBlocks) throws IOException {
		assertEquals(1, locatedBlocks.size());
		LocatedBlock locatedBlock = blockLocations.get(0);
		assertEquals(1, locatedBlock.getLocations().length);

		DatanodeInfo datanodeInfo = locatedBlock.getLocations()[0];
		ClientDatanodeProtocol createClientDatanodeProtocolProxy = HadoopFileLocationPrototypeTest
				.createClientDatanodeProtocolProxy(datanodeInfo,
						hadoopFileSystem.getConf(), 1000);

		BlockLocalPathInfo blockLocalPathInfo = createClientDatanodeProtocolProxy
				.getBlockLocalPathInfo(locatedBlock.getBlock(),
						locatedBlock.getBlockToken());
		String absolutePathToBlock = blockLocalPathInfo.getBlockPath();
		assertTrue(new File(absolutePathToBlock).exists());
		FileUtil.symLink(absolutePathToBlock, fileInDir.getAbsolutePath());
	}

	private Path copyRealBucketToHadoop(String hadoopHost, String hadoopPort)
			throws IOException {
		hadoopFileSystem = TUtilsFunctional.getHadoopFileSystem(hadoopHost,
				hadoopPort);
		Path bucketPathOnHadoop = new Path(testDataPath, realBucket.getName());
		hadoopFileSystem.copyFromLocalFile(new Path(realBucket.getDirectory()
				.toURI()), bucketPathOnHadoop);
		return bucketPathOnHadoop;
	}

	private File getShuttlThawLocation(String splunkUsername,
			String splunkPassword, String splunkHost, String splunkPort)
			throws IllegalIndexException {
		SplunkSettings splunkSettings = new SplunkSettings(
				new SplunkServiceParameters(splunkUsername, splunkPassword, splunkHost,
						splunkPort).getLoggedInService());
		return splunkSettings.getThawLocation("shuttl");
	}

}

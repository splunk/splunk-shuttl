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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.BlockLocalPathInfo;
import org.apache.hadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.net.NetUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Prototype for getting paths to a file where the datanode is local. Read the
 * '//' (comments) to understand more!
 */
public class HadoopFileLocationPrototypeTest {

	private DistributedFileSystem fileSystem;
	private ClientProtocol namenode;

	@AfterClass
	public void teardown() throws IOException {
		fileSystem.close();
	}

	/**
	 * Before running the test: <br/>
	 * <br/>
	 * 1. run `ant hadoop-setup`<br/>
	 * 2. run the following command in build-cache/hadoop: bin/hadoop fs -put
	 * ../../test/resources/splunk-buckets/SPLUNK_BUCKET/
	 * db_1336330530_1336330530_0 / <br/>
	 * <br/>
	 * Note: This will be automated soon!
	 */
	@Test(groups = { "prototype" })
	public void printPathToABlockOnHadoop() throws IOException {
		// Connect to hdfs. Needs to be HDFS because we're casting to
		// org.apache.hadoop.hdfs.DistributedFileSystem
		URI uri = URI.create("hdfs://localhost:9000");
		fileSystem = (DistributedFileSystem) FileSystem.get(uri,
				new Configuration());
		namenode = fileSystem.getClient().namenode;

		// Get the path to the bucket that's been put to hadoop.
		Path bucketPath = new Path("/db_1336330530_1336330530_0");
		assertTrue(fileSystem.exists(bucketPath));

		// path to any file in the bucket. Chose .csv because it's
		// readable/verifiable.
		String filePath = "/db_1336330530_1336330530_0/bucket_info.csv";

		// Get location of the blocks for the file.
		LocatedBlocks blockLocations = namenode.getBlockLocations(filePath, 0,
				Long.MAX_VALUE);
		// There exists only one block because of how everything is set up.
		LocatedBlock locatedBlock = blockLocations.getLocatedBlocks().get(0);
		Block block = locatedBlock.getBlock();
		// There exists only one node.
		DatanodeInfo datanodeInfo = locatedBlock.getLocations()[0];

		// Get a proxy to the Datanode containing the block. (This took a while to
		// figure out)
		ClientDatanodeProtocol createClientDatanodeProtocolProxy = createClientDatanodeProtocolProxy(
				datanodeInfo, fileSystem.getConf(), 1000);

		// Get the local block path. Requires two settings on the server side of
		// hadoop.
		// 1. dfs.client.read.shortcircuit : 'true'
		// 2. dfs.block.local-path-access.user : '<user running the tests (ie.
		// periksson)>'
		BlockLocalPathInfo blockLocalPathInfo = createClientDatanodeProtocolProxy
				.getBlockLocalPathInfo(block, locatedBlock.getBlockToken());
		// Printing the local path to the block, so we can access it!!
		System.out.println("BLOCK PATH: " + blockLocalPathInfo.getBlockPath()
				+ " !!!!!!!!!!!!!!!!!!");
	}

	// Ugly hacks start here ------->

	// Taken from org.apache.hadoop.hdfs.DFSClient
	/** Create {@link ClientDatanodeProtocol} proxy using kerberos ticket */
	public static ClientDatanodeProtocol createClientDatanodeProtocolProxy(
			DatanodeID datanodeid, Configuration conf, int socketTimeout)
			throws IOException {
		InetSocketAddress addr = NetUtils.createSocketAddr(datanodeid.getHost()
				+ ":" + datanodeid.getIpcPort());
		if (ClientDatanodeProtocol.LOG.isDebugEnabled()) {
			ClientDatanodeProtocol.LOG.info("ClientDatanodeProtocol addr=" + addr);
		}
		return (ClientDatanodeProtocol) RPC.getProxy(ClientDatanodeProtocol.class,
				ClientDatanodeProtocol.versionID, addr, conf,
				NetUtils.getDefaultSocketFactory(conf), socketTimeout);
	}

}

// ShepCLI.java
//
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

/**
 * @author xli
 *
 */

package com.splunk.shep.cli;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.splunk.shep.connector.HdfsIO;

public class ShepCLI {

    /**
     * @param args
     */

    private ShepConf conf = null;
    // private final String DefaultHadoopHome = new
    // String("/Users/xli/app/hadoop-0.20.2-cdh3u1");
    private final String DefaultHadoopHome = new String(
	    "/Users/xli/app/hadoop-0.20.205.0");

    public static void main(String[] args) throws Exception {
	// TODO Auto-generated method stub

	// System.out.println("Splunk Shep CLI");

	// Test shell command
	// runCmd("ls");

	int startIndex = 0;
	String confPath = new String("ShepCLI.conf");
	if (args.length > 2) {
	    if (args[0].equals("-conf")) {
		confPath = args[1];
		startIndex = 2;
	    }
	}

	ShepCLI cli = new ShepCLI(confPath);
	cli.parseArgs(args, startIndex);

    }

    public ShepCLI(String confFile) {
	conf = new ShepConf(confFile);
	init();
    }

    private void parseArgs(String[] args, int startIndex) throws Exception {
	if (args.length < 1) {
	    usage();
	}

	// Explicit check for help
	if ((args[startIndex].equals("-h"))
		|| (args[startIndex].equals("help"))
		|| (args[startIndex].equals("--h"))) {
	    usage();
	} else if (args[startIndex].equals("-get")) {
	    System.out.println("Run -get command.");
	    if ((args.length - startIndex) == 3)
		get(args[startIndex + 1], args[startIndex + 2]);
	    else {
		System.out.println("Incorrect arguments for get command.");
		usage();
	    }
	} else if (args[startIndex].equals("-ls")) {
	    // System.out.println("Run -ls command.");
	    if ((args.length - startIndex) == 2)
		list(args[startIndex + 1]);
	    else {
		list("/");
	    }
	} else if (args[startIndex].equals("-cat")) {
	    // System.out.println("Run -cat command.");
	    if ((args.length - startIndex) == 2)
		cat(args[startIndex + 1]);
	    else {
		System.out.println("Incorrect arguments for cat command.");
		usage();
	    }
	} else if (args[startIndex].equals("-tail")) {
	    // System.out.println("Run -tail command.");
	    if ((args.length - startIndex) == 2)
		tail(args[startIndex + 1]);
	    else if (((args.length - startIndex) == 3)
		    && (args[startIndex + 1].equals("-f")))
		tailFile(args[startIndex + 2]);
	    else {
		System.out.println("Incorrect arguments for tail command.");
		usage();
	    }
	} else if (args[startIndex].equals("-import")) {
	    // System.out.println("Run -cat command.");
	    if ((args.length - startIndex) == 2)
		importFile(args[startIndex + 1]);
	    else {
		System.out.println("Incorrect arguments for cat command.");
		usage();
	    }
	} else {
	    System.out.println("Unknown commands.");
	    usage();
	}
    }

    private void usage() {
	System.out.println("Usage : java " + getClass().getName() + " <cmd>");
	System.out
		.println("Commands:\n" + "    -h\n" + "    -ls <path>\n"
		+ "    -get <source-path> <dest-path>\n"
			+ "    -cat <source-path>\n"
			+ "    -tail [-f] <source-path>\n");
    }

    private void init() {
	if (conf.getHadoopHome() == null) {
	    // System.out.println("Using default Hadoop Home: "
	    // + DefaultHadoopHome);
	    conf.setHadoopHome(DefaultHadoopHome);
	} else {
	    // System.out.println("Hadoop home: " + conf.getHadoopHome());
	}
    }

    private void get(String src, String dest) throws Exception {
	Runtime r = Runtime.getRuntime();
	String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -get " + src + " "
		+ dest;

	// System.out.println("run: " + cmd);
	runCmd(cmd);
	// r.exec(cmd);
    }

    private void list(String src) throws Exception {
	Runtime r = Runtime.getRuntime();
	String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -ls " + src;

	// System.out.println("run: " + cmd);
	runCmd(cmd);
	// r.exec(cmd);
    }

    private void cat(String src) throws Exception {
	Runtime r = Runtime.getRuntime();
	String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -cat " + src;

	// System.out.println("run: " + cmd);
	runCmd(cmd);
	// r.exec(cmd);
    }

    private void importFile(String src) throws Exception {
	int index = src.lastIndexOf('/');
	if (index < 0)
	    index = 0;
	else
	    index += 1;
	String fileName = src.substring(index);
	Runtime r = Runtime.getRuntime();
	String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -get " + src
		+ conf.getSplunkHome() + "/etc/apps/shep/import/" + fileName;

	// System.out.println("run: " + cmd);
	runCmd(cmd);
	// r.exec(cmd);
    }

    private void tailFile(String src) throws Exception {
	HdfsIO fileIO = new HdfsIO(conf.getHadoopIP(), conf.getHadoopPort());
	if (!fileIO.setFilePath(src)) {
	    System.out.println("Cannot find file: " + src);
	    return;
	}

	long modTime = -1;
	while (true) {
	    long newModTime = fileIO.getFileModTime();
	    if (newModTime < 0) {
		System.out.println("Cannot trace file: " + src);
		break;
	    }

	    if (modTime == newModTime)
		continue;

	    modTime = newModTime;
	    // Runtime r = Runtime.getRuntime();
	    String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -tail " + src;
	    // System.out.println("run: " + cmd);
	    runCmd(cmd);
	    // r.exec(cmd);
	}
    }

    private void tail(String src) throws Exception {
	Runtime r = Runtime.getRuntime();
	String cmd = conf.getHadoopHome() + "/bin/hadoop dfs -tail " + src;
	runCmd(cmd);
    }

    private static void runCmd(String cmd) {
	try {
	    Runtime rt = Runtime.getRuntime();
	    Process proc = rt.exec(cmd);
	    InputStream stdin = proc.getInputStream();
	    InputStreamReader isr = new InputStreamReader(stdin);
	    BufferedReader br = new BufferedReader(isr);
	    String line = null;
	    // System.out.println("<OUTPUT>");
	    while ((line = br.readLine()) != null)
		System.out.println(line);
	    // System.out.println("</OUTPUT>");
	    int exitVal = proc.waitFor();
	    System.out.println("Command exitValue: " + exitVal);
	} catch (Throwable t) {
	    t.printStackTrace();
	}
    }

}

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

public class ShepCLI {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
	// TODO Auto-generated method stub

	System.out.println("Splunk Shep CLI");
	ShepCLI cli = new ShepCLI();
	cli.parseArgs(args);

	// run shell command
	// Runtime r = Runtime.getRuntime();
	// r.exec("mv /Users/xli/code/javadev/workspace/Transmitter/test/txt /Users/xli/code/javadev/workspace/Transmitter/test/newtxt");

    }

    protected void parseArgs(String[] args) throws Exception {
	if (args.length < 1) {
	    usage();
	}

	// Explicit check for help
	if ((args[0].equals("-h")) || (args[0].equals("help"))
		|| (args[0].equals("--h"))) {
	    usage();
	} else if (args[0].equals("-get")) {
	    if (args.length == 3)
		get(args[1], args[2]);
	    else {
		System.out.println("Incorrect command arguments.");
		usage();
	    }
	} else {
	    System.out.println("Unknown commands.");
	    usage();
	}
    }

    protected void usage() {
	System.out.println("Usage : java " + getClass().getName() + " <cmd>");
	System.out.println("Commands:\n" + "-h"
		+ "-get <source-path> <dest-path>");
    }

    protected void get(String src, String dest) throws Exception {
	Runtime r = Runtime.getRuntime();
	r.exec("/Users/xli/app/hadoop-0.20.2-cdh3u1/bin/hadoop dfs -get " + src
		+ " " + dest);
    }

}

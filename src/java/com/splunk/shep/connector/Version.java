// Splunk2Flume.java
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

package com.splunk.shep.connector;

import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;


public class Version {
	public static final String PROD_NAME="Hadoop Connector";
	public static String VERSION="0.1";
	public static String P4_NUMBER="99"; 
	public static String BUILD_DATE="08/05/2011 09:00:00 AM";
	public static String BRANCH="curent"; 
	public static String BUILD_MACHINE="mrt"; 
	
	public static final String VERSION_STR="version";
	public static final String P4_NUMBER_STR="revision";
	public static final String BUILD_DATE_STR="build_date";
	public static final String BRANCH_STR="branch"; 
	public static final String BUILD_MACHINE_STR="build_machine"; 
	
	public static String BUILD_INFO;
	
	static {
		try {
			java.util.Properties props = new java.util.Properties();
			props.load(Version.class.getResourceAsStream("version.properties"));
			VERSION = props.getProperty(VERSION_STR);
			P4_NUMBER = props.getProperty(P4_NUMBER_STR);
			BUILD_DATE = props.getProperty(BUILD_DATE_STR);
			BRANCH = props.getProperty(BRANCH_STR);
			BUILD_MACHINE = props.getProperty(BUILD_MACHINE_STR);
			BUILD_INFO=P4_NUMBER + ":" + BUILD_DATE + ":" + BRANCH + ":" + BUILD_MACHINE;
		} catch (Exception e) {
			// defaults are already so this can be ignored
		}
	}

	public static final String OPT_VER = "-ver";
	private static final String MSG_VER = "print short version information";
	
	public static final String OPT_FULLVER = "-fullversion";
	private static final String MSG_FULLVER = "print complete version information";
	
	public static final String OPT_HELP = "-help";
	private static final String MSG_HELP = "print help";
	
	public static void main(String[] args) {
		if (!handleCli(Version.class.getName(), args, System.out)) {
			usage(Version.class.getName(), System.err);
			return;
		}
	}
	
	public static boolean handleCli(String classname, String[] args, OutputStream os) {
		// Right now we can handle only one arg
		if (args.length < 1) {
			return false;
		}
		
		// There is at least one arg
		PrintStream ps = new PrintStream(os);
		String opt = args[0];
		if (handleVer(opt, ps)) {
			return true;
		} else if(handleFullVersion(opt, ps)) {
			return true;
		} else if (handleHelp(classname, opt, ps)) {
			return true;
		}
		return false;
	}

	public static void shortUsage(String classname, PrintStream ps) {
		ps.print("Usage : java " + classname + 
				" [" + OPT_VER + "]" + 
				" [" + OPT_FULLVER + "]"  + 
				" [" + OPT_HELP + "]");
	}
	public static void longUsage(PrintStream ps) {
		List<String> opts = new ArrayList<String>();
		opts.add(OPT_VER);
		opts.add(OPT_FULLVER);
		opts.add(OPT_HELP);
		int m = maxChars(opts) + 5;
		ps.print(
				String.format("%1$s %2$-" + (m-OPT_VER.length())+ "s - %3$s", OPT_VER, " ", MSG_VER) + "\n" +
				String.format("%1$s %2$-" + (m-OPT_FULLVER.length()) + "s - %3$s", OPT_FULLVER, " ", MSG_FULLVER) + "\n" +
				String.format("%1$s %2$-" + (m-OPT_HELP.length()) + "s - %3$s", OPT_HELP, " ", MSG_HELP) + "\n"
				);
	}
	private static void usage(String classname, PrintStream ps) {
		List<String> opts = new ArrayList<String>();
		opts.add(OPT_VER);
		opts.add(OPT_FULLVER);
		opts.add(OPT_HELP);
		int m = maxChars(opts) + 5;
		shortUsage(classname, ps);
		ps.println("\n\n" +
					String.format("%1$s %2$-" + (m-OPT_VER.length())+ "s - %3$s", OPT_VER, " ", MSG_VER) + "\n" +
					String.format("%1$s %2$-" + (m-OPT_FULLVER.length()) + "s - %3$s", OPT_FULLVER, " ", MSG_FULLVER) + "\n" +
					String.format("%1$s %2$-" + (m-OPT_HELP.length()) + "s - %3$s", OPT_HELP, " ", MSG_HELP) + "\n"
					);
	}
	
	private static boolean handleVer(String option, PrintStream ps) {
		if (option.equals(OPT_VER)) {
			ps.println(PROD_NAME + " " + VERSION);
			return true;
		}
		return false;
	}
	
	private static boolean handleFullVersion(String option, PrintStream ps) {
		if (option.equals(OPT_FULLVER)) {
			ps.println(PROD_NAME + " " + VERSION + " (" + BUILD_INFO + ")");
			return true;
		}
		return false;
	}
	
	private static boolean handleHelp(String classname, String option, PrintStream ps) {
		if (option.equals(OPT_HELP)) {
			usage(classname, ps);
			return true;
		}
		return false;
	}

	private static int maxChars(List<String> opts) {
		int max = 0;
		for (String o : opts) {
			if (max < o.length())
				max = o.length();
		}
		return max;
	}
}

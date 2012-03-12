package com.splunk.shep.testutil;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import com.splunk.shep.archiver.archive.BucketArchiver;

public class ShellClassRunner {

    private Integer exitCode = null;
    private List<String> stdOut;
    private List<String> stdErr;

    public ShellClassRunner runClassWithArgs(Class<?> clazz, String... args) {
	String fullClassName = clazz.getName();
	Process exec = doRunArchiveBucket(fullClassName, args);
	exitCode = getStatusCode(exec);
	stdOut = readInputStream(exec.getInputStream());
	stdErr = readInputStream(exec.getErrorStream());
	return this;
    }

    private Process doRunArchiveBucket(String fullClassName, String[] args) {
	try {
	    String execString = getJavaExecutablePath() + " -cp \":"
		    + getClasspath() + ":\" " + fullClassName + " "
		    + getSpaceSeparatedArgs(args);
	    return Runtime.getRuntime().exec(execString);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    /**
     * Choose a class that's in src and one that's in test, to get their class
     * paths.
     */
    private String getClasspath() {
	String srcClasspath = BucketArchiver.class.getProtectionDomain()
		.getCodeSource().getLocation().getPath();
	String testClasspath = ShellClassRunnerTest.class.getProtectionDomain()
		.getCodeSource().getLocation().getPath();
	String libClasspath = "lib/*";
	return srcClasspath + ":" + testClasspath + ":" + libClasspath;
    }

    private String getSpaceSeparatedArgs(String[] args) {
	StringBuilder sb = new StringBuilder();
	for (String s : args)
	    sb.append(s + " ");
	return sb.toString();
    }

    private int getStatusCode(Process exec) {
	try {
	    return exec.waitFor();
	} catch (Exception e) {
	    printStdOutAndErr(exec);
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    private void printStdOutAndErr(Process exec) {
	try {
	    System.err.println(IOUtils.readLines(exec.getErrorStream()));
	    System.err.println(IOUtils.readLines(exec.getInputStream()));
	} catch (IOException e) {
	    throw new RuntimeException(e);
	}
    }

    public Integer getExitCode() {
	return exitCode;
    }

    private List<String> readInputStream(InputStream inputStream) {
	try {
	    return IOUtils.readLines(inputStream);
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    public List<String> getStdOut() {
	return stdOut;
    }

    public List<String> getStdErr() {
	return stdErr;
    }

    /**
     * @return
     */
    /* package-private */String getJavaExecutablePath() {
	Map<String, String> env = System.getenv();
	if (env.containsKey("JAVA_HOME")) {
	    return env.get("JAVA_HOME") + "/bin/java";
	} else {
	    return "java";
	}
    }
}

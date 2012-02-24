package com.splunk.shep.testutil;

import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;

public class ShellClassRunner {

    private Integer exitCode = null;
    private List<String> stdOut;

    public ShellClassRunner runClassWithArgs(Class<?> clazz, String... args) {
	String classpath = clazz.getProtectionDomain().getCodeSource()
		.getLocation().getPath();
	String fullClassName = clazz.getName();

	Process exec = doRunArchiveBucket(classpath, fullClassName, args);
	exitCode = getStatusCode(exec);
	stdOut = readStdIn(exec);
	return this;
    }

    private Process doRunArchiveBucket(String classpath, String fullClassName,
	    String[] args) {
	try {
	    return Runtime.getRuntime().exec(
		    "java -cp " + classpath + " " + fullClassName + " "
			    + getSpaceSeparatedArgs(args));
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
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

    private List<String> readStdIn(Process exec) {
	try {
	    return IOUtils.readLines(exec.getInputStream());
	} catch (IOException e) {
	    e.printStackTrace();
	    throw new RuntimeException(e);
	}
    }

    public List<String> getStdOut() {
	return stdOut;
    }

}

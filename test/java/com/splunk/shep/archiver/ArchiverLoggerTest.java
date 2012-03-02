package com.splunk.shep.archiver;

import static org.testng.AssertJUnit.*;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class ArchiverLoggerTest {

    PrintWriter realPrintWriter;
    StringWriter logWriter;

    @BeforeClass
    public void beforeClass() {
	realPrintWriter = ArchiverLogger.logger;
    }

    @AfterClass
    public void afterClass() {
	ArchiverLogger.logger = realPrintWriter;
    }

    @BeforeMethod
    public void beforeMethod() {
	logWriter = new StringWriter();
	ArchiverLogger.logger = new PrintWriter(logWriter);
    }

    public void did_validArguments_expectedOutput() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff");
	String result = logWriter.toString();
	assertTrue(
		"Loged message was " + result,
		result.matches("\\[.+?\\] did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\"\n"));

    }

    public void did_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff", "btw",
		"200");
	String result = logWriter.toString();
	assertTrue(
		"Loged message was " + result,
		result.matches("\\[.+?\\] did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\" btw=\"200\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void did_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff",
		"more stuff");
    }

    public void done_noAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.done("doneStuff");
	String result = logWriter.toString();
	assertTrue("Loged message was " + result,
		result.matches("\\[.+?\\] done=\"doneStuff\"\n"));
    }

    public void done_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.done("doneStuff", "btw", "200");
	String result = logWriter.toString();
	assertTrue("Loged message was " + result,
		result.matches("\\[.+?\\] done=\"doneStuff\" btw=\"200\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void done_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.done("doneStuff", "btw");
    }

    public void will_validArguments_expectedOutput() {
	ArchiverLogger.will("willStuff");
	String result = logWriter.toString();
	assertTrue("Loged message was " + result,
		result.matches("\\[.+?\\] will=\"willStuff\"\n"));

    }

    public void will_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.will("willStuff", "args", "stuff stuff stuff");
	String result = logWriter.toString();
	assertTrue(
		"Loged message was " + result,
		result.matches("\\[.+?\\] will=\"willStuff\" args=\"stuff stuff stuff\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void will_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.will("willStuff", "btw");
    }
}

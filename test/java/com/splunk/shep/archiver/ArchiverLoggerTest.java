package com.splunk.shep.archiver;

import static org.testng.AssertJUnit.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Date;

import org.testng.annotations.AfterSuite;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

@Test(groups = { "fast" })
public class ArchiverLoggerTest {

    PrintWriter realPrintWriter;
    StringWriter logWriter;
    String assertFailedMessageStart;

    @BeforeSuite
    public void beforeClass() {
	realPrintWriter = ArchiverLogger.logger;
    }

    @AfterSuite
    public void afterClass() {
	ArchiverLogger.logger = realPrintWriter;
    }

    @BeforeMethod
    public void beforeMethod() {
	assertFailedMessageStart = "Logged message was ";
	logWriter = new StringWriter();
	ArchiverLogger.logger = new PrintWriter(logWriter);
    }

    public void did_validArguments_expectedOutput() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff");
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\"\n"));

    }

    public void did_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff", "btw",
		"200");
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] did=\"didStuff\" happened=\"happenedStuff\" expected=\"expectedStuff\" btw=\"200\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void did_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.did("didStuff", "happenedStuff", "expectedStuff",
		"more stuff");
    }

    public void did_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	ArchiverLogger.did(currentDate, "btw", 200, "btw", 5.6f);

	// Verify
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] did=\"" + currentDate.toString()
			+ "\" happened=\"btw\" expected=\"200\" btw=\"5.6\"\n"));
    }

    public void done_noAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.done("doneStuff");
	String result = logWriter.toString();
	assertTrue(assertFailedMessageStart + result,
		result.matches("\\[.+?\\] done=\"doneStuff\"\n"));
    }

    public void done_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.done("doneStuff", "btw", "200");
	String result = logWriter.toString();
	assertTrue(assertFailedMessageStart + result,
		result.matches("\\[.+?\\] done=\"doneStuff\" btw=\"200\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void done_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.done("doneStuff", "btw");
    }

    public void done_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	ArchiverLogger.done(currentDate, "btw", 200);

	// Verify
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] done=\"" + currentDate.toString()
			+ "\" btw=\"200\"\n"));
    }

    public void will_validArguments_expectedOutput() {
	ArchiverLogger.will("willStuff");
	String result = logWriter.toString();
	assertTrue(assertFailedMessageStart + result,
		result.matches("\\[.+?\\] will=\"willStuff\"\n"));

    }

    public void will_withAdditionalKeyValues_expectedOutput() {
	ArchiverLogger.will("willStuff", "args", "stuff stuff stuff");
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] will=\"willStuff\" args=\"stuff stuff stuff\"\n"));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void will_ilegalNumberOfArguments_illegalArgumentException() {
	ArchiverLogger.will("willStuff", "btw");
    }

    public void will_nonObjectArguments_expectedOutput() {
	Date currentDate = new Date();

	// Test
	ArchiverLogger.will("willStuff", currentDate, 5);

	// Verify
	String result = logWriter.toString();
	assertTrue(
		assertFailedMessageStart + result,
		result.matches("\\[.+?\\] will=\"willStuff\" "
			+ currentDate.toString() + "=\"5\"\n"));
    }
}

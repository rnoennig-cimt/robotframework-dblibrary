package org.robot.database.keywords.test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.robot.database.keywords.DatabaseKeywords;
import org.robot.database.keywords.DatabaseLibraryException;

/**
 * Tests that use a database connection
 */
public class DatabaseLibraryTest {

	private static final String HSQL_DRIVER_CLASSNAME = "org.hsqldb.jdbcDriver";
	private static final String HSQL_URL = "jdbc:hsqldb:mem:xdb";
	private static final String HSQL_USER = "sa";
	private static final String HSQL_PASSWORD = "";

	private DatabaseKeywords databaseKeywords;
	

	// ========================================================
	//
	// Setup and Teardown on class-level
	//
	// ========================================================

	@BeforeClass
	public static void setUp() throws Exception {
		Class.forName(HSQL_DRIVER_CLASSNAME).newInstance();
		Connection con = DriverManager.getConnection(HSQL_URL,
				HSQL_USER, HSQL_PASSWORD);

		Statement stmt = con.createStatement();
		stmt.execute("CREATE TABLE MySampleTable (Id Integer NOT NULL, Name VARCHAR(256), "
						+ "EMail VARCHAR(256), "
						+ "Postings Integer, State Integer, LastPosting Timestamp)");
		stmt.execute("CREATE TABLE EmptyTable (Id Integer, Name VARCHAR(256))");
		stmt.execute("CREATE TABLE ReferenceTable (Num Integer)");

		stmt.execute("ALTER TABLE MySampleTable ADD CONSTRAINT MyUniqKey UNIQUE (Id)");
		stmt.execute("ALTER TABLE MySampleTable ADD CONSTRAINT MyPrimKey PRIMARY KEY (Id)");
		
		stmt.close();
		con.close();
	}

	// ========================================================
	//
	// Setup and Teardown on method-level
	//
	// ========================================================

	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	@Before
	public void setUpTest() throws Exception {
		initTestTables();
		initDatabaseLibrary();
	}
	
	public void initTestTables() throws Exception {
		Class.forName(HSQL_DRIVER_CLASSNAME).newInstance();
		Connection con = DriverManager.getConnection(HSQL_URL,
				HSQL_USER, HSQL_PASSWORD);

		Statement stmt = con.createStatement();
		stmt.execute("DELETE FROM MySampleTable");
		stmt.execute("DELETE FROM EmptyTable");
		stmt.execute("DELETE FROM ReferenceTable");
		stmt
				.executeUpdate("INSERT INTO MySampleTable VALUES(1, 'Donny Darko', "
						+ "'donny.darko@robot.org', 1001, 1, '2010-02-26 12:42:58.0000')");
		stmt
				.executeUpdate("INSERT INTO MySampleTable VALUES(2, 'Darth Vader', "
						+ "'darth.vader@starwars.universe', 123, 2, '2010-02-27 11:41:45.0000')");

		stmt.executeUpdate("INSERT INTO ReferenceTable VALUES(1)");
		stmt.executeUpdate("INSERT INTO ReferenceTable VALUES(2)");

		stmt.close();
		con.close();
	}
	
	private void initDatabaseLibrary() throws Exception {
		databaseKeywords = new DatabaseKeywords();
		databaseKeywords.connectToDatabase("CONN01", HSQL_DRIVER_CLASSNAME,
				HSQL_URL, HSQL_USER, HSQL_PASSWORD);
	}	
	
	// ========================================================
	//
	// Check Table Must be Empty
	//
	// ========================================================

	@Test
	public void checktable_must_be_empty_OnEmptyTable() throws Exception {
		databaseKeywords.tableMustBeEmpty("EmptyTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_be_empty_OnTableNotEmpty() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustBeEmpty("MySampleTable");
	}

	// ========================================================
	//
	// Check Table must Exists
	//
	// ========================================================

	@Test
	public void checkTableMustExist_ThatExists() throws Exception {
		databaseKeywords.tableMustExist("EMPTYTABLE");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTableMustExist_ThatDoesNotExist() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustExist("WRONG_NAME");
	}

	// ========================================================
	//
	// Check Delete all Rows from Table
	//
	// ========================================================

	@Test
	public void checkdelete_all_rows_from_table() throws Exception {
		try {
			// Check first that table is not empty
			databaseKeywords.tableMustBeEmpty("MySampleTable");
			fail();
		} catch (DatabaseLibraryException e) {
			databaseKeywords.deleteAllRowsFromTable("MySampleTable");
			databaseKeywords.tableMustBeEmpty("MySampleTable");
		}
	}

	// ========================================================
	//
	// Check Table Must contain number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_number_of_rows() throws Exception {
		databaseKeywords.tableMustContainNumberOfRows("MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_number_of_rows_WrongNumber() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustContainNumberOfRows("MySampleTable", "5");
	}

	// ========================================================
	//
	// Check Table Must contain more than number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_more_than_number_of_rows() throws Exception {
		databaseKeywords.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "1");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_more_than_number_of_rows_SameNumbers() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_more_than_number_of_rows_MoreNumbers() throws Exception {
		databaseKeywords.tableMustContainMoreThanNumberOfRows(
				"MySampleTable", "200");
	}

	// ========================================================
	//
	// Check Table Must contain less than number of rows
	//
	// ========================================================

	@Test
	public void checktable_must_contain_less_than_number_of_rows() throws Exception {
		databaseKeywords.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "3");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_less_than_number_of_rows_SameNumbers() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "2");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checktable_must_contain_less_than_number_of_rows_LessNumbers() throws DatabaseLibraryException, Exception {
		databaseKeywords.tableMustContainLessThanNumberOfRows(
				"MySampleTable", "1");
	}

	// ========================================================
	//
	// Check Tables Must contain same amount of rows
	//
	// ========================================================

	@Test
	public void checkTablesMustContainSameAmountOfRows() throws Exception {
		databaseKeywords.tablesMustContainSameAmountOfRows("MySampleTable",
				"ReferenceTable");
	}

	@Test(expected=DatabaseLibraryException.class)
	public void checkTablesMustContainSameAmountOfRows_ButTheyDoNot() throws DatabaseLibraryException, Exception {
		databaseKeywords.tablesMustContainSameAmountOfRows("MySampleTable",
				"EmptyTable");
	}

	// ========================================================
	//
	// Check Check Content for row identified by Rownum
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyRownum() throws Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "1");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_WrongValues() throws DatabaseLibraryException, Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Dar|1001", "MySampleTable", "1");
	}	
	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyRownum_NoRecordFound() throws DatabaseLibraryException, Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByRownum("Id,Name,Postings",
						"1|Donny Dar|1001", "MySampleTable", "100");
	}		
	
	
	// ========================================================
	//
	// Check Check Content for row identified by where-clause
	//
	// ========================================================

	@Test
	public void checkCheckContentIdentifiedbyWhereClause() throws Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|1001", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_WrongValues() throws DatabaseLibraryException, Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=1");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_NoRecordFound() throws DatabaseLibraryException, Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=100");
	}	

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckContentIdentifiedbyWhereClause_MoreThanOneRecordFound() throws DatabaseLibraryException, Exception {
		databaseKeywords
				.checkContentForRowIdentifiedByWhereClause("Id,Name,Postings",
						"1|Donny Darko|100", "MySampleTable", "id=1 or id=2");
	}	

	
	
	// ========================================================
	//
	// Check Transaction Isolation Level
	//
	// ========================================================

	@Test
	public void checkGetTransactionIsolationLevel() throws Exception {
		String level = databaseKeywords.getTransactionIsolationLevel();
		System.out.println("Transaction Isolation Level: " + level);
		
		if ((level == null) || (level.equals(HSQL_PASSWORD))) {
			fail("Empty Transaction Isolation Level");
		}
	}	
	
	@Test
	public void checktransactionIsolationLevelMustBe() throws Exception {
		databaseKeywords.transactionIsolationLevelMustBe("TRANSACTION_READ_COMMITTED");
	}	
	
	@Test(expected=DatabaseLibraryException.class)
	public void checktransactionIsolationLevelMustBe_WithWrongLevelName() throws DatabaseLibraryException, Exception {
		databaseKeywords.transactionIsolationLevelMustBe("TRANSACTION_REPEATABLE_READ");
	}		
	
	
	// ========================================================
	//
	// Check Read Single Value from Table
	//
	// ========================================================

	@Test
	public void checkReadSingleValueFromTable() throws Exception {
		String name = databaseKeywords.readSingleValueFromTable("MySampleTable", "Name", "id=1");
		System.out.println("Single Value Fetched: " + name);
		Assert.assertEquals("Wrong value fetched", "Donny Darko", name);
	}
	
	@Test
	public void checkReadSingleValueFromTableReturnsEmptyStringIfNoMatch() throws SQLException, DatabaseLibraryException {
		String name = databaseKeywords.readSingleValueFromTable("MySampleTable", "Name", "id=23");
		System.out.println("Single Value Fetched: " + name);
		Assert.assertEquals("Value found", "", name);
	}

	
	// ========================================================
	//
	// Check Primary Key Column Information
	//
	// ========================================================

	@Test
	public void checkGetPrimaryKeyColumnsForTable() throws Exception {
		String keys = databaseKeywords.getPrimaryKeyColumnsForTable("MYSAMPLETABLE");
		System.out.println("Primary Keys: " + keys);
		
		if ((keys == null) || (keys.equals(HSQL_PASSWORD))) {
			fail("Empty Primary Key");
		}
	}		
	
	@Test
	public void checkCheckPrimaryKeyColumnsForTable() throws Exception {
		databaseKeywords.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Id");
	}			

	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_NoMatch() throws DatabaseLibraryException, Exception {
		databaseKeywords.checkPrimaryKeyColumnsForTable("MYSAMPLETABLE", "Ids");
	}			
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkCheckPrimaryKeyColumnsForTable_WrongTableName() throws DatabaseLibraryException, Exception {
		databaseKeywords.checkPrimaryKeyColumnsForTable("WrongTable", "Id");
	}
	
	
	//
	// Check Execute SQL
	//
	@Test
	public void checkExecuteSQL() throws Exception {
		databaseKeywords.executeSql("CREATE TABLE TestTable (Num Integer)");
		databaseKeywords.tableMustExist("TESTTABLE");
	}
    
    @Test
	public void checkExecuteSQLFromFile() throws Exception {
		URL url = this.getClass().getResource("/CreateTestTable.sql");
		assertNotNull(url);
		String filepath = new File(url.toURI()).getAbsolutePath();

		databaseKeywords.executeSqlFromFile(filepath);
		databaseKeywords.tableMustExist("TESTTABLEFROMFILE");
	}

	@Test
	public void checkExecuteSQLFromFilePreservingNewlinesWithMultilineString() throws Exception {
		URL url = this.getClass().getResource("/CreateTestTableWithMultilineString.sql");
		assertNotNull(url);
		String filepath = new File(url.toURI()).getAbsolutePath();

		databaseKeywords.executeSqlFromFilePreservingLineBreaks(filepath);
		databaseKeywords.tableMustExist("TESTTABLEMULTILINE");

		String message = databaseKeywords.readSingleValueFromTable("TESTTABLEMULTILINE", "Message", "id=1");

		Assert.assertEquals("Wrong value fetched", "from pyspark import SparkContext\n" +
				"\n" +
				"# This is a simple program.\n" +
				"if __name__ == \"__main__\":\n" +
				"  sc = SparkContext(appName=\"PySpark-Test\")\n" +
				"  sc.stop()\n" +
				"",
				message);
	}
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessOneMatch() throws Exception {
		databaseKeywords.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='donny.darko@robot.org'", "1");
	}
	
	@Test
	public void checkVerifyNumberOfRowsMatchingWhereSuccessNoMatch() throws Exception {
		databaseKeywords.verifyNumberOfRowsMatchingWhere("MySampleTable", "EMail='batman@robot.org'", "0");
	}
	
	@Test(expected=DatabaseLibraryException.class)
	public void checkVerifyNumberOfRowsMatchingWhereFailure() throws DatabaseLibraryException, Exception {
		databaseKeywords.verifyNumberOfRowsMatchingWhere("MySampleTable", "Postings > 0", "1");
	}

	// ========================================================
	//
	// Store Query Result To File
	//
	// ========================================================
	
	@Test 
	public void checkStoreQueryResultToFile() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";
		databaseKeywords.storeQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	    FileReader fr = new FileReader(myFileName); 
	    BufferedReader br = new BufferedReader(fr);
	    Assert.assertEquals("Wrong value written to file","Donny Darko|donny.darko@robot.org|",br.readLine());
	    Assert.assertEquals("Wrong value written to file","Darth Vader|darth.vader@starwars.universe|",br.readLine());
	    Assert.assertEquals("File is longer than expected",false, br.ready());
        br.close();
	}

	// ========================================================
	//
	// Compare Query Result To File
	//
	// ========================================================
	
	@Test 
	public void checkCompareQueryResultToFileWhereFileMatches() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";	
		FileWriter fstream = new FileWriter(myFileName);
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write("Donny Darko|donny.darko@robot.org|\n");
	    out.write("Darth Vader|darth.vader@starwars.universe|\n");
	    out.close();
		databaseKeywords.compareQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	}

	@Test (expected=DatabaseLibraryException.class)
	public void checkCompareQueryResultToFileWhereFileHasTooFewRows() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";	
		FileWriter fstream = new FileWriter(myFileName);
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write("Donny Darko|donny.darko@robot.org|\n");
	    out.close();
		databaseKeywords.compareQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	}

	@Test (expected=DatabaseLibraryException.class)
	public void checkCompareQueryResultToFileWhereFileHasTooManyRows() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";	
		FileWriter fstream = new FileWriter(myFileName);
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write("Donny Darko|donny.darko@robot.org|\n");
	    out.write("Darth Vader|darth.vader@starwars.universe|\n");
	    out.write("Darth Vader|darth.vader@starwars.universe|\n");
	    out.close();
		databaseKeywords.compareQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	}

	@Test (expected=DatabaseLibraryException.class)
	public void checkCompareQueryResultToFileWhereFileHasInvalidRow() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";	
		FileWriter fstream = new FileWriter(myFileName);
	    BufferedWriter out = new BufferedWriter(fstream);
	    out.write("Donny Darko|donny.darko@robot.org|\n");
	    out.write("Darth Vader|lukes.father@starwars.universe|\n");
	    out.close();
		databaseKeywords.compareQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	}

	@Test (expected=FileNotFoundException.class)
	public void checkCompareQueryResultToFileWhereFileNotFound() throws Exception {
		folder.getRoot();
		String myFileName = folder.getRoot().getPath() + File.separator + "myFile.txt";	
		databaseKeywords.compareQueryResultToFile("select name, email from mySampleTable order by Id", 
				myFileName);
	}
	
	// Tests for "Row Should Not Exist In Table"
	@Test
	public void checkRowShouldNotExistInTable() throws SQLException, DatabaseLibraryException {
		databaseKeywords.rowShouldNotExistInTable("MySampleTable", "Name='John Doe'");
	}
	
	@Test
	public void checkRowShouldNotExistInTableFailsIfRowExists() throws SQLException, DatabaseLibraryException {
		try {
			databaseKeywords.rowShouldNotExistInTable("MySampleTable", "Name='Darth Vader'");
			fail();
		} catch(DatabaseLibraryException e) {
			Assert.assertEquals("Row exists (but should not) for where-clause: Name='Darth Vader' in table: MySampleTable", e.getMessage());
		}
	}

}

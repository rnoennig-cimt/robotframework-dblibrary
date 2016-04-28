package org.robot.database.keywords;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Map;
import java.util.TimeZone;

import org.python.google.common.collect.Maps;
import org.robotframework.javalib.annotation.ArgumentNames;
import org.robotframework.javalib.annotation.RobotKeyword;
import org.robotframework.javalib.annotation.RobotKeywordOverload;
import org.robotframework.javalib.annotation.RobotKeywords;

@RobotKeywords
public class DatabaseKeywords {
	private Connection connection = null;
	private String currentConnection = null;

    private Map<String, Connection> connectionMap = Maps.newHashMap();

	@RobotKeyword("Establish a named connection to the database. It is mandatory for at least one connection to be established\n" +
    "before any of the other keywords can be used and should be ideally done during the suite setup phase.\n" +
	"It must be ensured that the JAR-file containing the given driver can be\n" +
	"found from the CLASSPATH when starting robot. Furthermore it must be\n" +
	"noted that the connection string is database-specific and must be valid\n" +
	"of course.\n" +
	"\n" +
	"Example:\n" +
	"| Connect To Database | MySQL Connection | com.mysql.jdbc.Driver | jdbc:mysql://my.host.name/myinstance | UserName | ThePassword |")
	@ArgumentNames({"connectionName", "driverClassName", "connectionString", "dbUser=", "dbPassword="})
	public void connectToDatabase(String connectionName, String driverClassName, String connectionString, String dbUser, String dbPassword)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
        if (connectionMap.containsKey(connectionName)) {
            throw new IllegalStateException("This connection has already been established, please close it");
        }
        Class.forName(driverClassName).newInstance();
        Connection conn = null;
        if(dbUser == null && dbPassword == null) {
        	conn = DriverManager.getConnection(connectionString);
        } else {
        	conn = DriverManager.getConnection(connectionString, dbUser, dbPassword);
        }
        connectionMap.put(connectionName, conn);
        setConnection(conn, connectionName);
	}

	@RobotKeywordOverload
	public void connectToDatabase(String connectionName, String driverClassName, String connectString)
            throws SQLException, InstantiationException, IllegalAccessException, ClassNotFoundException {
		connectToDatabase(connectionName, driverClassName, connectString, null, null);
	}

	@RobotKeyword("Switch between established connections. At any given moment only one connection can be active.\n" +
		    "Any keywords will be executed against the active connection.\n" +
		    "\n" +
		    "Example:\n" +
		    "| Switch Connection | Test Connection |")
	@ArgumentNames({"connectionName"})
    public void switchConnection(String connectionName) {
        if (connectionMap.containsKey(connectionName)) {
            setConnection(connectionMap.get(connectionName), connectionName);
        } else {
            throw new IllegalStateException(String.format("Could not find connection %s please ensure the " +
                    "connection exists", connectionName));
        }
    }

	@RobotKeyword("Releases the currently active connection. In addition this keyword will\n" +
		    "log any SQLWarnings that might have been occurred on the\n" +
		    "connection.\n" +
			"\n" +
			"Example:\n" +
			"| Disconnect from Database |")
	public void disconnectFromDatabase() throws SQLException {
		System.out.println("SQL Warnings on current connection: "
				+ getConnection().getWarnings());
    removeConnection().close();

	}

	@RobotKeyword("Releases all existing connections to the database.\n"
			+ "In addition this keyword will log any SQLWarnings that might have been occurred on the connection.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Disconnect from All Databases |")
    public void disconnectFromAllDatabase() throws SQLException {
    for (Iterator<Map.Entry<String, Connection>> it = connectionMap.entrySet().iterator(); it.hasNext();) {
      Map.Entry<String, Connection> entry = it.next();

      System.out.println(String.format("SQL Warnings on %s connection: ", entry.getKey()) + entry.getValue().getWarnings());

      entry.getValue().close();

      it.remove();
    }
    }

	@RobotKeyword("Checks that a table with the given name exists.\n"
			+ "If the table does not exist the test will fail.\n"
			+ "\n"
			+ "NOTE: Some database expect the table names to be written all in upper case letters to be found.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Table Must Exist | MySampleTable |")
	@ArgumentNames({"tableName"})
	public void tableMustExist(String tableName) throws SQLException, DatabaseLibraryException {

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getTables(null, null, tableName, null);
		try {
			if (!rs.next()) {
				throw new DatabaseLibraryException("Table: " + tableName
						+ " was not found");
			}
		} finally {
			rs.close();
		}
	}

	/**
	 * Checks that the given table has no rows. It is a convenience way of using
	 * the "Table Must Contain Number Of Rows" with zero for the amount of rows.
	 *
	 * Example:
	 * | Table Must Be Empty | MySampleTable |
	 *
	 * @throws DatabaseLibraryException
	 * @throws SQLException
	 */
	public void tableMustBeEmpty(String tableName) throws SQLException,
			DatabaseLibraryException {
		tableMustContainNumberOfRows(tableName, "0");
	}

	@RobotKeyword("Deletes the entire content of the given database table. This keyword is\n"
			+ "useful to start tests in a clean state. Use this keyword with care as\n"
			+ "accidently execution of this keyword in a productive system will cause\n"
			+ "heavy loss of data. There will be no rollback possible."
			+ "\n"
			+ "Example:\n"
			+ "| Delete All Rows From Table | MySampleTable |")
	@ArgumentNames({"tableName"})
	public void deleteAllRowsFromTable(String tableName) throws SQLException {
		String sql = "delete from " + tableName;

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sql);
		} finally {
			stmt.close();
		}
	}

	@RobotKeyword("This keyword checks that a given table contains a given amount of rows.\n"
			+ "For the example this means that the table \"MySampleTable\" must contain\n"
			+ "exactly 14 rows, otherwise the teststep will fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Table Must Contain Number Of Rows | MySampleTable | 14 |")
	@ArgumentNames({"tableName", "rowNumValue"})
	public void tableMustContainNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum
 + " rows in table '" + tableName + "', fetched: " + num);
		}
	}

	@RobotKeyword("This keyword checks that a given table contains more than the given amount of rows.\n"
			+ "For the example this means that the table \"MySampleTable\"\n"
			+ "must contain 100 or more rows, otherwise the teststep will fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Table Must Contain More Than Number Of Rows | MySampleTable | 99 |")
	@ArgumentNames({"tableName", "rowNumValue"})
	public void tableMustContainMoreThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum + 1);
		if (num <= rowNum) {
      throw new DatabaseLibraryException("Expecting more than " + rowNum + " rows in table '" + tableName + "', fetched: " + num);
		}
	}

	@RobotKeyword("This keyword checks that a given table contains less than the given amount of rows.\n"
			+ "For the example this means that the table \"MySampleTable\"\n"
			+ "must contain anything between 0 and 1000 rows, otherwise the teststep will fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Table Must Contain Less Than Number Of Rows | MySampleTable | 1001 |")
	@ArgumentNames({"tableName", "rowNumValue"})
	public void tableMustContainLessThanNumberOfRows(String tableName,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, rowNum);
		if (num >= rowNum) {
      throw new DatabaseLibraryException("Expecting less than " + rowNum + " rows in table '" + tableName + "', fetched: " + num);
		}
	}

	@RobotKeyword("This keyword checks that two given database tables have the same amount of rows.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Tables Must Contain Same Amount Of Rows | MySampleTable | MyCompareTable |")
	@ArgumentNames({"firstTableName", "secondTableName"})
	public void tablesMustContainSameAmountOfRows(String firstTableName,
			String secondTableName) throws SQLException,
			DatabaseLibraryException {

		long firstNum = getNumberOfRows(firstTableName);
		long secondNum = getNumberOfRows(secondTableName);

		if (firstNum != secondNum) {
			throw new DatabaseLibraryException(
					"Expecting same amount of rows, but table "
							+ firstTableName + " has " + firstNum
							+ " rows and table " + secondTableName + " has "
							+ secondNum + " rows!");
		}
	}

	@RobotKeyword("This keyword can be used to check for proper content inside a specific row in a database table.\n"
			+ "For this it is possible to give a\n"
			+ "comma-separated list of column names in the first parameter and a\n"
			+ "pipe-separated list of values in the second parameter. Then the name of\n"
			+ "the table and the rownum to check must be passed to this keyword. The\n"
			+ "corresponding values are then read from that row in the given table and\n"
			+ "compared to the expected values. If all values match the teststep will\n"
			+ "pass, otherwise it will fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Check Content for Row Identified by Rownum | Name,EMail | John Doe|john.doe@x-files | MySampleTable | 4 |")
	@ArgumentNames({"columnNames", "expectedValues", "tableName", "rowNumValue"})
	public void checkContentForRowIdentifiedByRownum(String columnNames,
			String expectedValues, String tableName, String rowNumValue)
			throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		String sqlString = "select " + columnNames + " from " + tableName;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sqlString);
			ResultSet rs = stmt.getResultSet();

			long count = 0;
			while (rs.next()) {

				count++;
				if (count == rowNum) {

					for (int i = 0; i < columns.length; i++) {
						String fieldValue = rs.getString(columns[i]);
						System.out.println(columns[i] + " -> " + fieldValue);

						if (values[i].equals("(NULL)")) {
							values[i] = "";
						}

						if (!fieldValue.equals(values[i])) {
							throw new DatabaseLibraryException("Value found: '"
									+ fieldValue + "'. Expected: '" + values[i]
									+ "'");
						}
					}
					break;
				}
			}

			// Rownum does not exist
			if (count != rowNum) {
				throw new DatabaseLibraryException(
						"Given rownum does not exist for statement: " + sqlString);
			}

		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("This keyword can be used to check for proper content inside a specific row in a database table.\n"
			+ "For this it is possible to give a\n"
			+ "comma-separated list of column names in the first parameter and a\n"
			+ "pipe-separated list of values in the second parameter. Then the name of\n"
			+ "the table and a statement used in the where-clause to identify a concrete\n"
			+ "row. The corresponding values are then read from the row identified this\n"
			+ "way and compared to the expected values. If all values match the teststep\n"
			+ "will pass, otherwise it will fail.\n"
			+ "\n"
			+ "If the where-clause will select more or less than exactly one row the test will fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Check Content for Row Identified by WhereClause | Name,EMail | John Doe|john.doe@x-files | MySampleTable | Postings=14 |")
	@ArgumentNames({"columnNames", "expectedValues", "tableName", "whereClause"})
	public void checkContentForRowIdentifiedByWhereClause(String columnNames,
			String expectedValues, String tableName, String whereClause)
			throws SQLException, DatabaseLibraryException {

		String sqlString = "select " + columnNames + " from " + tableName
				+ " where " + whereClause;

		String[] columns = columnNames.split(",");
		String[] values = expectedValues.split("\\|");

		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sqlString);
			ResultSet rs = stmt.getResultSet();

			long count = 0;
			while (rs.next()) {
				count++;
				if (count == 1) {

					for (int i = 0; i < columns.length; i++) {
						String fieldValue = rs.getString(columns[i]);
						System.out.println(columns[i] + " -> " + fieldValue);

						if (values[i].equals("(NULL)")) {
							values[i] = "";
						}

						if (!fieldValue.equals(values[i])) {
							throw new DatabaseLibraryException("Value found: '"
									+ fieldValue + "'. Expected: '" + values[i]
									+ "'");
						}
					}
				}

				// Throw exception if more than one row is selected by the given
				// "where-clause"
				if (count > 1) {
					throw new DatabaseLibraryException(
							"More than one row fetched by given where-clause for statement: "
									+ sqlString);
				}
			}

			// Throw exception if no row was fetched by given where-clause
			if (count == 0) {
				throw new DatabaseLibraryException(
						"No row fetched by given where-clause for statement: "
								+ sqlString);
			}

		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("Reads a single value from the given table and column based on the where-clause passed to the test.\n"
			+ "If the where-clause identifies more or\n"
			+ "less than exactly one row in that table this will result in an error for\n"
			+ "this teststep. Otherwise the selected value will be returned.\n"
			+ "\n"
			+ "Example:\n"
			+ "| ${VALUE}= | Read single Value from Table | MySampleTable | EMail | Name='John Doe' |")
	@ArgumentNames({"tableName", "columnName", "whereClause"})
	public String readSingleValueFromTable(String tableName, String columnName, String whereClause) throws SQLException, DatabaseLibraryException {
		String ret = "";

		String sql = "SELECT " + columnName + " FROM " + tableName + " WHERE "
				+ whereClause;
		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sql);
			ResultSet rs = stmt.getResultSet();

            if(rs.next()) {
				ret = rs.getString(columnName);
			}

			if (rs.next()) {
				throw new DatabaseLibraryException("More than one value fetched for: " + sql);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}

		return ret;
	}

	@RobotKeyword("Can be used to check that the database connection used for executing tests has the proper transaction isolation level.\n"
			+ "The string parameter\n"
			+ "accepts the following values in a case-insensitive manner:\n"
			+ "TRANSACTION_READ_UNCOMMITTED, TRANSACTION_READ_COMMITTED,\n"
			+ "TRANSACTION_REPEATABLE_READ, TRANSACTION_SERIALIZABLE or\n"
			+ "TRANSACTION_NONE.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Transaction Isolation Level Must Be | TRANSACTION_READ_COMMITTED |")
	@ArgumentNames({"levelName"})
	public void transactionIsolationLevelMustBe(String levelName)
			throws SQLException, DatabaseLibraryException {

		String transactionName = getTransactionIsolationLevel();

		if (!transactionName.equals(levelName)) {
			throw new DatabaseLibraryException(
					"Expected Transaction Isolation Level: " + levelName
							+ " Level found: " + transactionName);
		}

	}

	@RobotKeyword("Returns a String value that contains the name of the transaction isolation level of the connection that is used for executing the tests.\n"
			+ "Possible return values are: TRANSACTION_READ_UNCOMMITTED,\n"
			+ "TRANSACTION_READ_COMMITTED, TRANSACTION_REPEATABLE_READ,\n"
			+ "TRANSACTION_SERIALIZABLE or TRANSACTION_NONE.\n"
			+ "\n"
			+ "Example:\n"
			+ "| ${TI_LEVEL}= | Get Transaction Isolation Level |")
	public String getTransactionIsolationLevel() throws SQLException {

		String ret = "";

		int transactionIsolation = getConnection().getTransactionIsolation();

		switch (transactionIsolation) {

		case Connection.TRANSACTION_NONE:
			ret = "TRANSACTION_NONE";
			break;

		case Connection.TRANSACTION_READ_COMMITTED:
			ret = "TRANSACTION_READ_COMMITTED";
			break;

		case Connection.TRANSACTION_READ_UNCOMMITTED:
			ret = "TRANSACTION_READ_UNCOMMITTED";
			break;

		case Connection.TRANSACTION_REPEATABLE_READ:
			ret = "TRANSACTION_REPEATABLE_READ";
			break;

		case Connection.TRANSACTION_SERIALIZABLE:
			ret = "TRANSACTION_SERIALIZABLE";
			break;
		}

		return ret;
	}

	@RobotKeyword("Checks that the primary key columns of a given table match the columns given as a comma-separated list.\n"
			+ "Note that the given list must be ordered\n"
			+ "by the name of the columns. Upper and lower case for the columns as such\n"
			+ "is ignored by comparing the values after converting both to lower case.\n"
			+ "\n"
			+ "NOTE: Some database expect the table names to be written all in upper case letters to be found.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Check Primary Key Columns For Table | MySampleTable | Id,Name |")
	@ArgumentNames({"tableName", "columnList"})
	public void checkPrimaryKeyColumnsForTable(String tableName,
			String columnList) throws SQLException, DatabaseLibraryException {

		String keys = getPrimaryKeyColumnsForTable(tableName);

		columnList = columnList.toLowerCase();
		keys = keys.toLowerCase();

		if (!columnList.equals(keys)) {
			throw new DatabaseLibraryException("Given column list: "
					+ columnList + " Keys found: " + keys);
		}
	}

	@RobotKeyword("Returns a comma-separated list of the primary keys defined for the given table.\n"
			+ "The list if ordered by the name of the columns.\n"
			+ "\n"
			+ "NOTE: Some database expect the table names to be written all in upper case letters to be found.\n"
			+ "\n"
			+ "Example:\n"
			+ "| ${KEYS}= | Get Primary Key Columns For Table | MySampleTable |")
	@ArgumentNames({"tableName"})
	public String getPrimaryKeyColumnsForTable(String tableName)
			throws SQLException {

		String ret = "";

		DatabaseMetaData dbm = getConnection().getMetaData();
		ResultSet rs = dbm.getPrimaryKeys(null, null, tableName);
		try {
			while (rs.next()) {
				ret = rs.getString("COLUMN_NAME") + ",";
			}
		} finally {
			rs.close();
		}

		// Remove the last ","
		if (ret.length() > 0) {
			ret = ret.substring(0, ret.length() - 1);
		}

		return ret;
	}

	@RobotKeyword("Executes the given SQL without any further modifications.\n"
			+ "The given SQL must be valid for the database that is used. The main purpose of this\n"
			+ "keyword is building some contents in the database used for later testing.\n"
			+ "\n"
			+ "NOTE: Use this method with care as you might cause damage to your\n"
			+ "database, especially when using this in a productive environment.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Execute SQL | CREATE TABLE MyTable (Num INTEGER) |")
	@ArgumentNames({"sqlString"})
	public void executeSql(String sqlString) throws SQLException {

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
		} finally {
			stmt.close();
		}
	}
	@RobotKeyword("Executes the SQL statements contained in the given file without the modification in no transaction\n"
			+ "The given SQL must be valid for the database that\n"
			+ "is used. Any lines prefixed with \"REM\" or \"#\" are ignored. This keyword\n"
			+ "can for example be used to setup database tables from some SQL install\n"
			+ "script.\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but\n"
			+ "must be terminated with a semicolon \";\". A new statement must always\n"
			+ "start in a new line and not in the same line where the previous statement\n"
			+ "was terminated by a \";\".\n"
			+ "In case there is a problem in executing any of the SQL statements from\n"
			+ "the file the execution is terminated and the operation is rolled back.\n"
			+ "\n"
			+ "NOTE: Use this method with care as you might cause damage to your\n"
			+ "database, especially when using this in a productive environment.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Execute SQL from File | myFile.sql |")
	@ArgumentNames({"fileName"})
	public void executeSqlFromFileWithoutTransaction(String fileName) throws SQLException,
			IOException, DatabaseLibraryException {
//we dont need a transaction
//		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			line = line.trim();

			// Ignore lines commented out in the given file
			if (line.toLowerCase().startsWith("rem")) {
				continue;
			}
			if (line.startsWith("#")) {
				continue;
			}

			// Add the line to the current SQL statement
			sql += " ";
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1).trim();
					System.out.println("Executing: " + sql);
					executeSql(sql);
					sql = "";
				}
			} catch (SQLException e) {
				sql = "";
				br.close();

				throw new DatabaseLibraryException("Error executing: " + sql
						+ " Execution from file ! " + e.getMessage());
			}
		}

//		getConnection().commit();
//		getConnection().setAutoCommit(true);
		br.close();
	}

	@RobotKeyword("Executes the SQL statements contained in the given file without any further modifications.\n"
			+ "The given SQL must be valid for the database that\n"
			+ "is used. Any lines prefixed with \"REM\" or \"#\" are ignored. This keyword\n"
			+ "can for example be used to setup database tables from some SQL install\n"
			+ "script.\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but\n"
			+ "must be terminated with a semicolon \";\". A new statement must always\n"
			+ "start in a new line and not in the same line where the previous statement\n"
			+ "was terminated by a \";\".\n"
			+ "In case there is a problem in executing any of the SQL statements from\n"
			+ "the file the execution is terminated and the operation is rolled back.\n"
			+ "\n"
			+ "NOTE: Use this method with care as you might cause damage to your\n"
			+ "database, especially when using this in a productive environment.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Execute SQL from File | myFile.sql |")
	@ArgumentNames({"fileName"})
	public void executeSqlFromFile(String fileName) throws SQLException,
			IOException, DatabaseLibraryException {

		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			line = line.trim();

			// Ignore lines commented out in the given file
			if (line.toLowerCase().startsWith("rem")) {
				continue;
			}
			if (line.startsWith("#")) {
				continue;
			}

			// Add the line to the current SQL statement
			sql += " ";
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1).trim();
					System.out.println("Executing: " + sql);
					executeSql(sql);
					sql = "";
				}
			} catch (SQLException e) {
				sql = "";
				br.close();
				getConnection().rollback();
				getConnection().setAutoCommit(true);
				throw new DatabaseLibraryException("Error executing: " + sql
						+ " Execution from file rolled back!", e);
			}
		}

		getConnection().commit();
		getConnection().setAutoCommit(true);
		br.close();
	}

	@RobotKeyword("Executes the SQL statements contained in the given file without any further modifications.\n"
			+ "The given SQL must be valid for the database that\n"
			+ "is used. Any lines prefixed with \"REM\" or \"#\" are ignored. This keyword\n"
			+ "can for example be used to setup database tables from some SQL install\n"
			+ "script.\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but\n"
			+ "must be terminated with a semicolon \";\". A new statement must always\n"
			+ "start in a new line and not in the same line where the previous statement\n"
			+ "was terminated by a \";\".\n"
			+ "Any errors that might happen during execution of SQL statements are\n"
			+ "logged to the Robot Log-file, but otherwise ignored.\n"
			+ "\n"
			+ "NOTE: Use this method with care as you might cause damage to your\n"
			+ "database, especially when using this in a productive environment.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Execute SQL from File | myFile.sql |")
	@ArgumentNames({"fileName"})
	public void executeSqlFromFileIgnoreErrors(String fileName)
			throws SQLException, IOException, DatabaseLibraryException {

		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			line = line.trim();

			// Ignore lines commented out in the given file
			if (line.toLowerCase().startsWith("rem")) {
				continue;
			}
			if (line.startsWith("#")) {
				continue;
			}

			// Add the line to the current SQL statement
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1);
					System.out.println("Executing: " + sql + "\n");
					executeSql(sql);
					sql = "";
					System.out.println("\n");
				}
			} catch (SQLException e) {
				System.out.println("Error executing: " + sql + "\n"
						+ e.getMessage() + "\n\n");
				sql = "";
			}
		}

		getConnection().commit();
		getConnection().setAutoCommit(true);
		br.close();
	}

	@RobotKeyword("Executes the SQL statements contained in the given file without any further modifications.\n"
			+ "The given SQL must be valid for the database that\n"
			+ "is used. This keyword\n"
			+ "can for example be used to setup database tables from some SQL install\n"
			+ "script.\n"
			+ "Single SQL statements in the file can be spread over multiple lines, but\n"
			+ "must be terminated with a semicolon \";\". A new statement must always\n"
			+ "start in a new line and not in the same line where the previous statement\n"
			+ "was terminated by a \";\".\n"
			+ "Any line breaks and whitespaces inside single SQL statements are preserved.\n"
			+ "\n"
			+ "In case there is a problem in executing any of the SQL statements from\n"
			+ "the file the execution is terminated and the operation is rolled back.\n"
			+ "\n"
			+ "NOTE: Use this method with care as you might cause damage to your\n"
			+ "database, especially when using this in a productive environment.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Execute SQL From File Preserving Line Breaks| myFile.sql |")
	@ArgumentNames({"fileName"})
	public void executeSqlFromFilePreservingLineBreaks(String fileName)
			throws SQLException, IOException, DatabaseLibraryException {

		getConnection().setAutoCommit(false);

		FileReader fr = new FileReader(new File(fileName));
		BufferedReader br = new BufferedReader(fr);

		String sql = "";
		String line = "";
		while ((line = br.readLine()) != null) {
			// Add the line to the current SQL statement
			if (!sql.isEmpty()) {
				sql += "\n";
			}
			sql += line;

			// Check if SQL statement is complete, if yes execute
			try {
				if (sql.endsWith(";")) {
					sql = sql.substring(0, sql.length() - 1).trim();
					System.out.println("Executing: " + sql);
					executeSql(sql);
					sql = "";
				}
			} catch (SQLException e) {
				br.close();
				getConnection().rollback();
				getConnection().setAutoCommit(true);
				throw new DatabaseLibraryException("Error executing: " + sql
						+ " Execution from file rolled back!", e);
			}
		}

		getConnection().commit();
		getConnection().setAutoCommit(true);
		br.close();
	}

	@RobotKeyword("This keyword checks that a given table contains a given amount of rows matching a given WHERE clause.\n"
			+ "For the example this means that the table \"MySampleTable\" must contain\n"
			+ "exactly 2 rows matching the given WHERE, otherwise the teststep will\n"
			+ "fail.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Verify Number Of Rows Matching Where | MySampleTable | email=x@y.net | 2 |")
	@ArgumentNames({"tableName", "where", "rowNumValue"})
	public void verifyNumberOfRowsMatchingWhere(String tableName, String where,
			String rowNumValue) throws SQLException, DatabaseLibraryException {

		long rowNum = Long.valueOf(rowNumValue);

		long num = getNumberOfRows(tableName, where, (rowNum + 1));
		if (num != rowNum) {
			throw new DatabaseLibraryException("Expecting " + rowNum
					+ " rows, fetched: " + num);
		}
	}

  private Connection removeConnection() {
    if (connection == null) {
      throw new IllegalStateException("No connection open. Did you forget to run 'Connect To Database' before?");
    }
    return connectionMap.remove(currentConnection);
  }

	@RobotKeyword("This keyword can be used to check the inexistence of content inside a specific row in a database table defined by a where-clause.\n"
			+ "This can be used to validate an exclusion of specific data from a table.\n"
			+ "\n"
			+ "Example:\n"
			+ "| Row Should Not Exist In Table | MySampleTable | Name='John Doe' |\n"
			+ "\n"
			+ "This keyword was introduced in version 1.1.")
	@ArgumentNames({"tableName", "where", "rowNumValue"})
	public void rowShouldNotExistInTable(String tableName, String whereClause)
		throws SQLException, DatabaseLibraryException {

		String sql = "select * from " + tableName + " where " + whereClause;
		Statement stmt = getConnection().createStatement();
		try {
			stmt.executeQuery(sql);
			ResultSet rs = stmt.getResultSet();
			if(rs.next() == true) {
				throw new DatabaseLibraryException("Row exists (but should not) for where-clause: "
						+ whereClause + " in table: " + tableName);
			}
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("Executes the given SQL without any further modifications and stores the result in a file.\n"
			+ "The SQL query must be valid for\n"
			+ "the database that is used. The main purpose of this keyword\n"
			+ "is to generate expected result sets for use with keyword\n"
			+ "compareQueryResultToFile\n"
			+ "\n"
			+ "Example:\n"
			+ "| Store Query Result To File | Select phone, email from addresses where last_name = 'Johnson' | query_result.txt |")
	@ArgumentNames({"sqlString", "fileName"})
	public void storeQueryResultToFile(String sqlString, String fileName) throws SQLException, IOException {

		Statement stmt = getConnection().createStatement();
		try {
			stmt.execute(sqlString);
			ResultSet rs = stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
		    int numberOfColumns = rsmd.getColumnCount();
		    FileWriter fstream = new FileWriter(fileName);
		    BufferedWriter out = new BufferedWriter(fstream);
			while (rs.next()) {
				for (int i = 1; i <= numberOfColumns; i++) {
					rs.getString(i);
					out.write(rs.getString(i) + '|');
				}
				out.write("\n");
			}
			out.close();
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
		}
	}

	@RobotKeyword("Executes the given SQL compares the result to expected results stored in a file.\n"
			+ "Results are stored as strings\n"
			+ "separated with pipes ('|') with a pipe following the last column.\n"
			+ "Rows are separated with a newline.\n"
			+ "\n"
			+ "To ensure compares work correctly\n"
			+ "The SQL query should\n"
			+ "a) specify an order\n"
			+ "b) convert non-string fields (especially dates) to a specific format\n"
			+ "\n"
			+ "storeQueryResultToFile can be used to generate expected result files\n"
			+ "\n"
			+ "Example:\n"
			+ "| Compare Query Result To File | Select phone, email from addresses where last_name = 'Johnson' | query_result.txt |")
	@ArgumentNames({"sqlString", "fileName"})
	public void compareQueryResultToFile(String sqlString, String fileName) throws SQLException, DatabaseLibraryException, FileNotFoundException {

		Statement stmt = getConnection().createStatement();
	    int numDiffs = 0;
	    int maxDiffs = 10;
	    String diffs = "";
	    try {
			stmt.execute(sqlString);
			ResultSet rs = stmt.getResultSet();
			ResultSetMetaData rsmd = rs.getMetaData();
		    int numberOfColumns = rsmd.getColumnCount();
		    FileReader fr = new FileReader(fileName);
		    BufferedReader br = new BufferedReader(fr);
		    String actRow;
		    String expRow;

		    int row = 0;
			while (rs.next() && (numDiffs < maxDiffs)) {
				actRow = "";
				row++;
				for (int i = 1; i <= numberOfColumns; i++) {
					actRow += rs.getString(i) + '|';
				}
				expRow = br.readLine();
				if (!actRow.equals(expRow)) {
					numDiffs++;
					diffs += "Row " + row + " does not match:\nexp: " + expRow + "\nact: " +actRow + "\n";
				}
			}
			if (br.ready() && numDiffs < maxDiffs) {
				numDiffs++;
				diffs += "More rows in expected file than in query result\n";
			}
			br.close();
		} catch (FileNotFoundException e) {
			throw e;
		} catch (IOException e) {
			numDiffs++;
			diffs += "Fewer rows in expected file than in query result\n";
		} finally {
			// stmt.close() automatically takes care of its ResultSet, so no rs.close()
			stmt.close();
			if (numDiffs > 0) throw new DatabaseLibraryException(diffs);
		}
	}

	@RobotKeyword("Set a Java system property\n"
			+ "Example:\n"
			+ "| Set System Property | sun.security.jgss.debug | false |")
	@ArgumentNames({"key", "value"})
	public void setSystemProperty(String key, String value) {
		System.setProperty(key, value);
	}


	private void setConnection(Connection connection, String name) {
		this.connection = connection;
		this.currentConnection = name;
	}

	private Connection getConnection() {
		if (connection == null) {
			throw new IllegalStateException("No connection open. Did you forget to run 'Connect To Database' before?");
		}
		return connection;
	}


	private long getNumberOfRows(String tableName) throws SQLException {
		return getNumberOfRows(tableName, Long.MAX_VALUE);
	}

	private long getNumberOfRows(String tableName, long limit) throws SQLException {
		return getNumberOfRows(tableName, null, limit);
	}

	/*
	 * @param limit Limit is used to cut off counting in case count(*) is not supported
	 */
	private long getNumberOfRows(String tableName, String where, long limit)
			throws SQLException {

		// Let's first try with count(*), but this is not supported by all
		// databases.
		// In this case an exception will be thrown and we will read the amount
		// of records the "hard way", but luckily limited by the amount of rows
		// expected,
		// so that this might not be too bad.
		long num = -1;
		try {
			String sql = "select count(*) from " + tableName;
			if (where != null) {
				sql = sql + " where " + where;
			}
			Statement stmt = getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = stmt.getResultSet();
				rs.next();
				num = rs.getLong(1);
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no rs.close()
				stmt.close();
			}
		} catch (SQLException e) {
			System.out.println("Error while executing select count(*) in getNumberOfRows(). Fall back to select *");
			e.printStackTrace();
			String sql = "select * from " + tableName;
			if (where != null) {
				sql = sql + " where " + where;
			}
			Statement stmt = getConnection().createStatement();
			try {
				stmt.executeQuery(sql);
				ResultSet rs = stmt.getResultSet();
				num = 0;
				while ((rs.next()) && (num < limit)) {
					num++;
				}
			} finally {
				// stmt.close() automatically takes care of its ResultSet, so no rs.close()
				stmt.close();
			}
		}
		return num;
	}

	@RobotKeyword("Get the name of the connection currently in use.\n"
			+ "The connection returned may be null, if the connection was closed before.\n"
			+ "Example:\n"
			+ "| ${conn}= | Get Current Connection Name |\n"
			+ "| Switch Connection | Impala |\n"
			+ "| Log | Doing something senseful with the new connection |\n"
			+ "| Switch Connection | ${conn} | # restore connection |")
	public String getCurrentConnectionName() {
		return this.currentConnection;
	}

	@RobotKeyword("Load the given CSV file and inserts for each row the entries into the given table using dynamically determined data types.\n"
			+ "No header should be contained, csv contents should be separated by |\n"
			+ "Null Values should be set as null or (null).\n"
			+ "Example:\n"
			+ "| Import Table From File | table | /path/to/table.csv  |")
	@ArgumentNames({ "tableName", "file" })
	public void importTableFromFile(String tableName, String file)
			throws SQLException, IOException {

		// execute simple select to retrieve only metadata
		PreparedStatement typestmt = getConnection().prepareStatement("SELECT * FROM " + tableName + " WHERE 1=0");
		ResultSet typeres = typestmt.executeQuery();
		ResultSetMetaData rsmd = typeres.getMetaData();

		String posList = "?";
		for (int j = 1; j < rsmd.getColumnCount(); j++) {
			posList += ",?";
		}

		String sql = "INSERT	INTO " + tableName + "\r\n"
				+ "VALUES	\r\n" + "(" + posList + ")";

		PreparedStatement stmt = getConnection().prepareStatement(sql);

		FileReader fr = new FileReader(new File(file));
		BufferedReader br = null;

		try {
			br = new BufferedReader(fr);

			String line = "";
			// insert row by row, null values are handled with different setter, check correct column count
			while ((line = br.readLine()) != null) {
				String[] values = line.split("\\|");

				if (rsmd.getColumnCount() != values.length) {
					throw new IndexOutOfBoundsException();
				}

				for (int i = 0; i < values.length; i++) {
					if ("NULL".equals(values[i].toUpperCase()) ||
						"(NULL)".equals(values[i].toUpperCase())) {
						values[i] = null;
					}

					if (values[i] != null) {
						stmt.setObject(i+1, values[i]);
					} else {
						stmt.setNull(i+1, Types.NULL);
					}
				}

				stmt.execute();
			}
		} finally {
			if (br != null) {
				br.close();
			}
			stmt.close();
			typestmt.close();
		}
	}
}

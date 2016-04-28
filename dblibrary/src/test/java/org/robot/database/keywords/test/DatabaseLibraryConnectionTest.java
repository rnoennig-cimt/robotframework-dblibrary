package org.robot.database.keywords.test;

import java.sql.SQLException;
import java.util.Map;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.python.google.common.collect.ImmutableMap;
import org.robot.database.keywords.DatabaseKeywords;



/**
 * Tests related to connecting to the database
 */
public class DatabaseLibraryConnectionTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final Map<String, String> db1Properties = ImmutableMap.of(
            "driver", "org.hsqldb.jdbcDriver",
            "url", "jdbc:hsqldb:mem:xdb",
            "user", "sa",
            "password", ""
    );

    private static final Map<String, String> db2Properties = ImmutableMap.of(
            "driver", "org.hsqldb.jdbcDriver",
            "url", "jdbc:hsqldb:mem:testDb",
            "user", "sa",
            "password", ""
    );

    private DatabaseKeywords databaseKeywords;

	@Before
	public void setUpTest() throws Exception {
		databaseKeywords = new DatabaseKeywords();
	}

	// ========================================================
	//
	// Database Connection 
	//
	// ========================================================

    @Test
    public void checkConnectToDatabaseWithName() throws Exception {
        databaseKeywords.connectToDatabase("Test db", db2Properties.get("driver"),
                db2Properties.get("url"),db2Properties.get("user"),db2Properties.get("password"));
    }

    @Test
    public void switchDatabases() throws Exception {
        databaseKeywords.connectToDatabase("Test db1", db2Properties.get("driver"),
                db2Properties.get("url"),db2Properties.get("user"),db2Properties.get("password"));
        databaseKeywords.connectToDatabase("Test db2", db2Properties.get("driver"),
                db2Properties.get("url"),db2Properties.get("user"),db2Properties.get("password"));

        databaseKeywords.switchConnection("Test db2");
    }

    @Test
	public void checkConnectToDatabaseWithWrongUsername() throws Exception{
	    expectedException.expect(SQLException.class);
        expectedException.expectMessage("not found");
        databaseKeywords.connectToDatabase("CONN01", db1Properties.get("driver"), db1Properties.get("url"), "user", db1Properties
                .get("password"));
 	}

	@Test
	public void checkDisconnectFromDatabase() throws Exception {
        databaseKeywords.connectToDatabase("CONN01", db1Properties.get("driver"),
                db1Properties.get("url"),db1Properties.get("user"),db1Properties.get("password"));
		databaseKeywords.disconnectFromDatabase();
	}

	@Test
	public void checkIllegalStateExceptionWithoutConnect() throws Exception {
        expectedException.expect(IllegalStateException.class);
        databaseKeywords.tableMustBeEmpty("NoConnection");
	}
}
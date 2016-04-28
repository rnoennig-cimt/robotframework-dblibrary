package org.robot.database.keywords.test;

import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.net.URL;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.robot.database.keywords.DatabaseKeywords;

public class FramekeycacheTest {
	
	private static final String TERA_DRIVER_CLASSNAME = "com.teradata.jdbc.TeraDriver";
	private DatabaseKeywords dbKeywords;
	@Before
	public void setUp() throws Exception {
		System.out.println("teraUser=" + System.getProperty("teraUser"));
		
		dbKeywords = new DatabaseKeywords();
		dbKeywords.connectToDatabase(
				"TERA01",
				TERA_DRIVER_CLASSNAME,
				"jdbc:teradata://teradev/DATABASE="
						+ System.getProperty("teraDatabase") + ",CHARSET=UTF8",
				System.getProperty("teraUser"),
				System.getProperty("teraPassword"));
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	@Ignore
	public void testReturnGeneratedJobInstanceId() throws Exception {
		URL url = this.getClass().getResource("/FramekeycacheTest.csv");
		assertNotNull(url);
		String filepath = new File(url.toURI()).getAbsolutePath();
		String jobName = "testJenkinsInsert";
		dbKeywords.importFramekeycacheFromFile(jobName, filepath);
	}
	
}



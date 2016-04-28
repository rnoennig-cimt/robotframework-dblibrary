package org.robot.database;

import org.robotframework.javalib.library.AnnotationLibrary;

public class DatabaseLibrary extends AnnotationLibrary {
	private static final String KEYWORD_PATTERN = "org/robot/database/keywords/**/*.class";
	public static final String ROBOT_LIBRARY_SCOPE = "GLOBAL";

	/**
	 * Default-constructor
	 */
	public DatabaseLibrary() {
		addKeywordPattern(KEYWORD_PATTERN);
	}

    @Override
    public String getKeywordDocumentation(String keywordName) {
        if (keywordName.equals("__intro__"))
            return getIntro();
        return super.getKeywordDocumentation(keywordName);
    }

	private String getIntro() {
		return "This library supports database-related testing using the Robot Framework. It\n" +
"allows to establish a connection to a certain database to perform tests on\n" +
"the content of certain tables and/or views in that database. A possible\n" +
"scenario for its usage is a Web-Application that is storing data to the\n" +
"database based on some user actions (probably a quite common scenario). The\n" +
"actions in the Web-Application could be triggered using some tests based on\n" +
"Selenium and in the same test it will then be possible to check if the proper\n" +
"data has ended up in the database as expected. Of course there are various\n" +
"other scenarios where this library might be used.\n" +
"\n" +
"As this library is written in Java support for a lot of different database\n" +
"systems is possible. This only requires the corresponding driver-classes\n" +
"(usually in the form of a JAR from the database provider) and the knowledge\n" +
"of a proper JDBC connection-string.\n" +
"\n" +
"The following table lists some examples of drivers and connection strings\n" +
"for some popular databases.\n" +
"| *Database* | *Driver Name* | *Sample Connection String* | *Download Driver* |\n" +
"| MySql | com.mysql.jdbc.Driver | jdbc:mysql://servername/dbname | http://dev.mysql.com/downloads/connector/j/ |\n" +
"| Oracle | oracle.jdbc.driver.OracleDriver | jdbc:oracle:thin:@servername:port:dbname | http://www.oracle.com/technology/tech/java/sqlj_jdbc/htdocs/jdbc_faq.html |\n" +
"\n" +
"The examples in the description of the keywords is based on a database table\n" +
"named 'MySampleTable' that has the following layout:\n" +
"\n" +
"MySampleTable:\n" + 
"| *COLUMN* | *TYPE* |\n" +
"| Id | Number |\n" +
"| Name | String |\n" +
"| EMail | String |\n" +
"| Postings | Number |\n" +
"| State | Number |\n" +
"| LastPosting | Timestamp |\n" +
"\n" +
"NOTE: A lot of keywords that are targeted for Tables will work equally with\n" +
"Views as this is often no difference if Select-statements are performed.";
	}
}
package br.com.abevieiramota.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionFactory {
	
	public static Connection instance() throws SQLException, ClassNotFoundException {
		
		Class.forName("org.hsqldb.jdbcDriver");
		return DriverManager.getConnection("jdbc:hsqldb:mem:mydb;shutdown=true");
	}

}

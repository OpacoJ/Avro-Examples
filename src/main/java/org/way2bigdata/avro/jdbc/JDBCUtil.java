package org.way2bigdata.avro.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCUtil {
	
	// Util method for registring driver
	static void registerDriver(){
	      try {
			Class.forName("com.mysql.jdbc.Driver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * @param url url for the database connection
	 * @param user username of database
	 * @param password password for database
	 */
	static Connection openConnection(String url, String user, String password){
		Connection conn = null;
	    try {
			conn = DriverManager.getConnection(url,user,password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return conn;
	}
	
	static Statement createStatement(Connection conn){
		Statement stmt = null;
	    try {
			stmt = conn.createStatement();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return stmt;
	}
	
	
	static ResultSet executeQuery(String sql, Statement stmt){
		ResultSet rs = null;
	    try {
			rs = stmt.executeQuery(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rs;

	}

}

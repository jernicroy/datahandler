package com.asirtech.datahandler;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

/**
 * Creates an Database connection to PostgreSql using JDBC
 * 
 */
public class DbConnection {
	/**
	 * 
	 * @return
	 * Establish the jdbc connection to database and
	 * returns the connection string 
	 */
	public Connection connectDb() {
		
		Properties p= new Properties();
		InputStream inp;
		Connection con= null; 
		try {
			inp = new FileInputStream("src\\main\\resource\\application.properties");
			p.load(inp);
			
			String dbUrl = p.getProperty("db.URL");
			String user = p.getProperty("db.User");
			String password = p.getProperty("db.password");
			
			System.out.println("DB Username: "+ p.getProperty("db.User"));
			
			con =  DriverManager.getConnection(dbUrl, user, password);
			
			if(con!=null) {
				System.out.println("Connection Successful");
			}else {
				System.out.println("Connection Failed");
			}
			
			
			
		} catch (Exception e) {
			e.printStackTrace();
		} 
		return con;
	}
}

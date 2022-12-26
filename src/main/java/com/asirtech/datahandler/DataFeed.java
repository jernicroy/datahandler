package com.asirtech.datahandler;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;

import com.opencsv.CSVReader;

/**
 * @implNote
 * Here the implementation of  Insert Query<br>
 * retrieves data from CSV file by "CSVReader" and feeds the data into tables using INSERT Query
 */
public class DataFeed {

	CSVReader reader;
	
	/**
	 * Feed data into Tables in PostgreSQL
	 * @param file
	 */
	public void dataFeed(File file, Connection con) {
		try {
			reader = new CSVReader(new FileReader(file));
			String[] line;
			line = reader.readNext();
			int l= line.length;
			String tablename= file.getName().replace(".csv", "");
			String sql = "INSERT INTO " + tablename +"(" + line[0];
		    for(int i=1;i<l;i++) {
		    	sql += "," + line[i];
		    }
		    sql += ") VALUES (?";
		    for(int j=1;j<l;j++) {
		    	sql += ", ?";
		    }
		    sql+=");";
//			String sql = ""+ file + "(reg_no, first_name, last_name, location) VALUES (?, ?, ?, ?);";
			PreparedStatement statement= con.prepareStatement(sql);
			System.out.println(sql);
			
			while((line= reader.readNext())!= null) {
				int count = line.length;
				int reg= Integer.parseInt(line[0]);
				statement.setInt(1, reg);
				for(int i=1;i<count;i++) {
					statement.setString(i+1, line[i]);
				}
//				statement.setString(2, line[1]);
//				statement.setString(3, line[2]);
//				statement.setString(4, line[3]);
				statement.executeUpdate();
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}

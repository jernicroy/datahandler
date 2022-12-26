package com.asirtech.datahandler;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import com.opencsv.CSVReader;
/**
 * Establish database connection using <code>DbConnection</code> class.<br>
 * Retrieves the csv Data files and feed the data into tables 
 * 
 *
 *
 */
public class CsvFileList {
	public static DataFeed df = new DataFeed();
	DbConnection d = new DbConnection();
	Connection con= d.connectDb();
	/**
	 * Retrieves all the files and list them 
	 */
	public void csvFileList() {
		try {
		
			final File folder = new File("src\\main\\resource\\data");
			listFiles(folder, con);
		
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	/**
	 * @implNote Checks the existence of the data in the directory.<br>
	 * Examine the respective tables present in the database.<br>
	 * If table doesn't present, Create New table in DB <b>(PostgreSQL)</b>.
	 * @param folder - csv data files
	 * @param con - db connection string
	 */
	public static void listFiles(final File folder, Connection con) {
	
		int count =1;
		for(final File fileEntry : folder.listFiles() ) {
			if(fileEntry.isDirectory()) {
				listFiles(fileEntry,con);
			} else {
				
				System.out.println("\nTable " + count +": " + fileEntry.getName());
				
				DatabaseMetaData dmd;
				String tempFilename;
				tempFilename =fileEntry.getName().replace(".csv", "");
				
				try {
					dmd = con.getMetaData();
					ResultSet tables = dmd.getTables(null,null,tempFilename,null);
					if(tables.next()) {
						System.out.println("Table Exists");
						df.dataFeed(fileEntry, con);
						count++;
					}
					else {
						
//						tempFilename += ".csv";
//						System.out.println(tempFilename);
						
						CSVReader reader = new CSVReader(new FileReader(fileEntry));
						String[] line = reader.readNext();
						int l = line.length;
						String sql = "CREATE TABLE " + tempFilename+"(" + line[0] +" INT";
					    for(int i=1;i<l;i++) {
					    	sql += "," + line[i]+ " TEXT";
					    }
					    sql += ");";
					    
						Statement statement = con.createStatement();
						int i=statement.executeUpdate(sql);
								
								
//								+ line[0] + "INT"
//								+ line[1] + "TEXT"
//								+ line[2] + "TEXT"
//								+ line[3]
//								+ ")";
						if(i==0) {
							System.out.println("The table '" + tempFilename + "' created successfully.");
							df.dataFeed(fileEntry, con);
							count++;
						}
						
						
					}
//					System.out.println(tables);f
				} catch (Exception e) {
					e.printStackTrace();
				}
		    	
			}
		}
	}
}

import java.io.*;
import java.util.Random;
import com.sleepycat.db.*;

public class BerkleyDBClient {

	//variable for if the database has been loaded or not
	private static boolean DatabaseLoaded = false;
	//database type: btree or hash or indexfile
	private static String DatabaseTypeFromUser = null;
	//databse location
	private static final String DB_TABLE = "/tmp/nstoik1_db/db_table";
	//number of records in the database
	private static final int NO_RECORDS = 100;
	//Database object
	private static Database my_table;
	//Cursor object
	private static Cursor myCursor = null;

	/*
	 * Author: Nelson
	 */
	static void createAndPopulate() {
		
		if (DatabaseLoaded) {
			System.out.println("A database has already been created at the given location");
			System.out.println("Please destroy the database before creating a new one");
			return;
		}
		
		try {

			// Create the database object.
			DatabaseConfig dbConfig = new DatabaseConfig();
			
			if (DatabaseTypeFromUser.compareToIgnoreCase("btree") == 0) {
				dbConfig.setType(DatabaseType.BTREE);
			} else if (DatabaseTypeFromUser.compareToIgnoreCase("hash") == 0) {
				dbConfig.setType(DatabaseType.HASH);
			} else if (DatabaseTypeFromUser.compareToIgnoreCase("indexfile") == 0) {
				//TODO: set up config file for indexfile
				System.out.println("Still need to create the indexfile table");
			}
			dbConfig.setAllowCreate(true);

			my_table = new Database(DB_TABLE, null, dbConfig);
			System.out.println(DB_TABLE + " has been created");

			/* populate the new database with NO_RECORDS records */
			populateTable(NO_RECORDS);
			System.out.println(NO_RECORDS + " records inserted into" + DB_TABLE);
			
			DatabaseLoaded = true;

		} catch (Exception e1) {
			System.err.println("createAndPopulate failed: " + e1.toString());
			DatabaseLoaded = false;
		}
	}

	/*
	 * Author: Leah
	 */
	static void retriveRecordsByKey() {
	}

	/*
	 * Author: Jim
	 */
	static void retriveRecordsByData() {
	}

	/*
	 * Author: Jim
	 */
	static void retriveRecordsByRange() {
	}

	/*
	 * Author: Nelson
	 * Closes the database and then removes it from disk as well
	 */
	static void destoryDatabase() {

		if(!DatabaseLoaded) {
			System.out.println("No database to destroy");
			return;
		}
		try {
			/* Close the database and the db environment */
			closeDB();

			/* to remove the table */
			my_table.remove(DB_TABLE, null, null);
			
			System.out.println(DB_TABLE + " closed and destroyed succesfully.");
			DatabaseLoaded = false;
			return;

		} catch (Exception e1) {
			System.err.println("destroyDatabase failed: " + e1.toString());
		}
	}
	
	
	/* 
	 * returns the path of the database location
	 */
	public static String getTableLocation() {
		return DB_TABLE;
	}
	
	/*
	 * sets the private variable DatabaseTypeFromUser to the user input at
	 * system startup
	 */
	public static void setDatabaseType(String newDatabaseType) {
		
		DatabaseTypeFromUser = newDatabaseType;
		System.out.println("Databse type is: " + DatabaseTypeFromUser);
	}
	
	/*
	 * closes the database connection.
	 * Called when the system is exiting or destroying the database
	 * If the database is not destroyed, the database can be reopened later
	 */
	public static void closeDB() {
		if(!DatabaseLoaded){
			return;
		}
		
		try {
			my_table.close();
		} catch (DatabaseException e) {
			e.printStackTrace();
			System.out.println("Error closing Database!");
		}
	}
	/*
	 * if the database is not destroyed during the last session, it can
	 * be reopened and used again
	 */
	public static boolean tryOpenExsistingDatabase() {
		
		try {
			my_table = new Database(DB_TABLE, null, null);
			DatabaseLoaded = true;
			return true;
			
		} catch (FileNotFoundException e) {
			DatabaseLoaded = false;
			return false;
		} catch (DatabaseException e) {
			System.out.println("Database Exception");
			DatabaseLoaded = false;
			return false;
		}
		
	}
	

	// -------------------------HELPER FUNCTIONS----------------------//
	
	private static void writeToFile(String key, String value) {
		try
		{
		    String filename= "answers.txt";

		    FileWriter fw = new FileWriter(filename,true); //the true will append the new data
		    fw.write(key + "\n");
		    fw.write(value + "\n");
		    fw.write("\n");
		    fw.close();
		}
		catch(IOException ioe)
		{
		    System.err.println("IOException: " + ioe.getMessage());
		}
	}


	
	/*
	 * To pouplate the given table with nrecs records
	 */
	private static void populateTable(int nrecs) {

		int range;
		DatabaseEntry kdbt, ddbt;
		String s;

		/*
		 * generate a random string with the length between 64 and 127,
		 * inclusive.
		 * 
		 * Seed the random number once and once only.
		 */
		Random random = new Random(1000000);

		try {
			for (int i = 0; i < nrecs; i++) {

				/* to generate a key string */
				range = 64 + random.nextInt(64);
				s = "";
				for (int j = 0; j < range; j++)
					s += (new Character((char) (97 + random.nextInt(26))))
							.toString();

				/* to create a DBT for key */
				kdbt = new DatabaseEntry(s.getBytes());
				kdbt.setSize(s.length());

				// to print out the key/data pair
				//System.out.println("KEY: " + s);

				/* to generate a data string */
				range = 64 + random.nextInt(64);
				s = "";
				for (int j = 0; j < range; j++)
					s += (new Character((char) (97 + random.nextInt(26))))
							.toString();
				// to print out the key/data pair
				//System.out.println("DATA: " + s);
				//System.out.println("");

				/* to create a DBT for data */
				ddbt = new DatabaseEntry(s.getBytes());
				ddbt.setSize(s.length());

				/* to insert the key/data pair into the database */
				my_table.putNoOverwrite(null, kdbt, ddbt);
			}
		} catch (DatabaseException dbe) {
			System.err.println("Populate the table: " + dbe.toString());
			System.exit(1);
		}
	}

}

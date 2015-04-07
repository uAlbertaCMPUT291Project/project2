import java.io.*;
import java.util.Random;
import com.sleepycat.db.*;

public class BerkleyDBClient {

	//boolean about whether the databases have been created or not
	private static boolean DatabaseCreated = false;
	// database type: btree or hash or indexfile
	private static String DatabaseTypeFromUser = null;
	// Hash table Database location
	private static final String HASH_TABLE = "/tmp/nstoik1_db/hash_table";
	// BTREE table database location
	private static final String BTREE_TABLE = "/tmp/nstoik1_db/btree_table";
	// number of records in the database
	private static final int NO_RECORDS = 100000;
	// Database object
	private static Database hash_table;
	// Secondary Database object
	private static Database btree_table;
	// Cursor object
	private static Cursor hash_cursor;
	// Cursor object
	private static Cursor btree_cursor;

	/*
	 * Author: Nelson
	 */
	static void createAndPopulate() {

		try {			
			// Create the hash database object.
			DatabaseConfig hash_dbConfig = new DatabaseConfig();
			hash_dbConfig.setType(DatabaseType.HASH);
			hash_dbConfig.setAllowCreate(true);
			hash_table = new Database(HASH_TABLE, null, hash_dbConfig);
			
			// Create the hash database object.
			DatabaseConfig btree_dbConfig = new DatabaseConfig();
			btree_dbConfig.setType(DatabaseType.BTREE);
			btree_dbConfig.setAllowCreate(true);
			btree_table = new Database(BTREE_TABLE, null, btree_dbConfig);
			
			//create the cursor's
			hash_cursor = hash_table.openCursor(null, null);
			btree_cursor = btree_table.openCursor(null, null);

			/* populate the new database with NO_RECORDS records */
			populateTable(NO_RECORDS);
			System.out.println(NO_RECORDS + " records inserted into the table");
			
			DatabaseCreated = true;

		} catch (Exception e1) {
			System.err.println("createAndPopulate failed: " + e1.toString());
			System.exit(-1);
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

		if(!DatabaseCreated) {
			return;
		}
		try {
			hash_table.close();
			btree_table.close();
		
			hash_table.remove(HASH_TABLE, null, null);
			btree_table.remove(BTREE_TABLE, null, null);

			System.out.println("Database closed and destroyed succesfully.");
			DatabaseCreated = false;
			return;

		} catch (Exception e1) {
			System.err.println("destroyDatabase failed: " + e1.toString());
		}
	}

	/*
	 * sets the private variable DatabaseTypeFromUser to the user input at
	 * system startup
	 */
	public static void setDatabaseType(String newDatabaseType) {

		DatabaseTypeFromUser = newDatabaseType;
		System.out.println("Databse type is: " + DatabaseTypeFromUser);
	}

	// -------------------------HELPER FUNCTIONS----------------------//

	private static void writeToFile(String key, String value) {
		try {
			String filename = "answers.txt";

			//true will append the new data to the end of the file
			FileWriter fw = new FileWriter(filename, true);
			fw.write(key + "\n");
			fw.write(value + "\n");
			fw.write("\n");
			fw.close();
		} catch (IOException ioe) {
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
		OperationStatus status;

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
				// System.out.println("KEY: " + s);

				/* to generate a data string */
				range = 64 + random.nextInt(64);
				s = "";
				for (int j = 0; j < range; j++)
					s += (new Character((char) (97 + random.nextInt(26))))
							.toString();
				// to print out the key/data pair
				// System.out.println("DATA: " + s);
				// System.out.println("");

				/* to create a DBT for data */
				ddbt = new DatabaseEntry(s.getBytes());
				ddbt.setSize(s.length());

				/* to insert the key/data pair into the database */
				status = hash_table.putNoOverwrite(null, kdbt, ddbt);
				status = btree_table.putNoOverwrite(null, kdbt, ddbt);

				if (status.toString().equalsIgnoreCase(
						"OperationStatus.KEYEXIST")) {
					System.out.println("Key already exists, skipping this key value pair");
					i--;
				}
			}
		} catch (DatabaseException dbe) {
			System.err.println("Populate the table: " + dbe.toString());
			System.exit(1);
		}
	}

}

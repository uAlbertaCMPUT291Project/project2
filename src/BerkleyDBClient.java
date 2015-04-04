import java.util.Random;
import com.sleepycat.db.*;

public class BerkleyDBClient {

	// to specify the file name for the table
	private static final String SAMPLE_TABLE = "/tmp/nstoik1_db/sample_table";
	private static final int NO_RECORDS = 100;

	/*
	 * Author: Nelson
	 */
	static void createAndPopulate() {
		try {

			// Create the database object.

			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setType(DatabaseType.BTREE);
			dbConfig.setAllowCreate(true);
			//dbConfig.setSortedDuplicates(true);

			Database my_table = new Database(SAMPLE_TABLE, null, dbConfig);
			System.out.println(SAMPLE_TABLE + " has been created");

			/* populate the new database with NO_RECORDS records */
			populateTable(my_table, NO_RECORDS);
			System.out.println(NO_RECORDS + " records inserted into"
					+ SAMPLE_TABLE);

			/* cloase the database and the db enviornment */
			my_table.close();

			/* to remove the table */
			my_table.remove(SAMPLE_TABLE,null,null);

		} catch (Exception e1) {
			System.err.println("Test failed: " + e1.toString());
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
	 */
	static void destoryDatabase() {
	}

	// -------------------------HELPER FUNCTIONS----------------------//

	/*
	 * To pouplate the given table with nrecs records
	 */
	static void populateTable(Database my_table, int nrecs) {
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
				 System.out.println("KEY: " + s);

				/* to generate a data string */
				range = 64 + random.nextInt(64);
				s = "";
				for (int j = 0; j < range; j++)
					s += (new Character((char) (97 + random.nextInt(26))))
							.toString();
				// to print out the key/data pair
				System.out.println("DATA: " + s);
				System.out.println("");

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

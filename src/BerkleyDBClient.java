import java.io.*;
import java.util.Random;
import java.util.Scanner;

import com.sleepycat.db.*;

public class BerkleyDBClient {


	// Hash table Database location
	private static final String HASH_TABLE = "/tmp/nstoik1_db/hash_table";
	// BTREE table database location
	private static final String BTREE_TABLE = "/tmp/nstoik1_db/btree_table";
	// BTREE table database location
	private static final String INDEX_TABLE = "/tmp/nstoik1_db/index_table";
	// number of records in the database
	private static final int NO_RECORDS = 100000;

	// database type: btree or hash or indexfile
	private String databaseTypeFromUser = null;
	//Database object
	private  Database hash_table;
	//Database object
	private  Database btree_table;
	//Database object
	private  Database index_table;
	// Cursor object
	private  Cursor hash_cursor;
	// Cursor object
	private  Cursor btree_cursor;
	// Cursor object
	private  Cursor index_cursor;

	
	public BerkleyDBClient (String DatabaseTypeFromUser){
		try {	
			//remove all previous data before starting doing anything
			try{
				Database.remove(HASH_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}
			try{
				Database.remove(BTREE_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}
			try{
				Database.remove(INDEX_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}
			
			// Create the hash database object.
			DatabaseConfig hash_dbConfig = new DatabaseConfig();
			hash_dbConfig.setType(DatabaseType.HASH);
			hash_dbConfig.setAllowCreate(true);
			hash_table = new Database(HASH_TABLE, null, hash_dbConfig);
			
			// Create the btree database object.
			DatabaseConfig btree_dbConfig = new DatabaseConfig();
			btree_dbConfig.setType(DatabaseType.BTREE);
			btree_dbConfig.setAllowCreate(true);
			btree_table = new Database(BTREE_TABLE, null, btree_dbConfig);
			
			// Create the index database object.
			DatabaseConfig index_dbConfig = new DatabaseConfig();
			index_dbConfig.setType(DatabaseType.HASH);
			index_dbConfig.setAllowCreate(true);
			index_table = new Database(INDEX_TABLE, null, index_dbConfig);
			
			//create the cursor's
			hash_cursor = hash_table.openCursor(null, null);
			btree_cursor = btree_table.openCursor(null, null);
			index_cursor = index_table.openCursor(null, null);
			
			this.databaseTypeFromUser = DatabaseTypeFromUser;
			System.out.println("The database type is: " + this.databaseTypeFromUser);
		}catch (Exception e) {
			System.err.println("BerkleyDBClient initlization failed: " + e.toString());
			System.exit(-1);
		}
	}
	
	
	/*
	 * Author: Nelson
	 */
	public void populate() {
		try {
			populateTable(NO_RECORDS);
			System.out.println(NO_RECORDS + " records inserted into the table");		
		} catch (Exception e) {
			System.err.println("populate failed: " + e.toString());
			System.exit(-1);
		}
	}

	/*
	 * Author: Leah
	 * Jim worked on this a little bit, cause he felt he should try walking before running
	 */
	public void retriveRecordsByKey() {
     
		 DatabaseEntry key = new DatabaseEntry();
	     DatabaseEntry data = new DatabaseEntry();
	     Database currentDatabase;
		 //============== If btree selected ==============
	     if (databaseTypeFromUser.equals("btree")){		
	    	 currentDatabase = btree_table;
	     }else{		
	     //============== If hash or index selected ==============
	    	 currentDatabase = hash_table; 
	     }
	     Scanner scanner = new Scanner(System.in);
	     System.out.println("Please enter a key: ");
	     String userInputKeyString = scanner.nextLine();
	
	     key.setData(userInputKeyString.getBytes( )); 
	     key.setSize(userInputKeyString.length( ));
	     
	     OperationStatus status = OperationStatus.NOTFOUND;
	     long startTime;
	     long endTime;
	     long durationInMicroSecond;
	     startTime =  System.nanoTime();

	     try {
			status = currentDatabase.get(null, key, data, LockMode.DEFAULT);
		} catch (DatabaseException e) {
			System.err.println("retriveRecordsByKey failed: " + e.toString());
			System.exit(-1);
		}
		endTime = System.nanoTime();
	     if (status == OperationStatus.SUCCESS){
	    	 System.out.println("Retrived 1 record.");
	    	 writeToFile(key,data);
	     }else{
	    	 System.out.println("Retrived 0 record. No Record found");
	     }
	     durationInMicroSecond = (endTime - startTime)/1000;

    	 System.out.println("Took: "+durationInMicroSecond+" micro second");
	}
	/*
	 * Author: Jim
	 */
	public void retriveRecordsByData() {
		try {
			Cursor currentCursor;
			DatabaseEntry key = new DatabaseEntry();
		    DatabaseEntry data = new DatabaseEntry();	 
		    Scanner scanner = new Scanner(System.in);
		    System.out.println("Please enter data value: ");
		    String userInputDataString = scanner.nextLine();
		    int numberOfRecordRetrived = 0;
		    
		    long startTime;
		    long endTime;
		    long durationInMicroSecond;
		    startTime =  System.nanoTime();
		    OperationStatus status;
		     //============== If hash or btree selected ==============
		    if (databaseTypeFromUser.equals("hash")||databaseTypeFromUser.equals("btree")){
		    	if (databaseTypeFromUser.equals("hash")){			
					currentCursor = hash_cursor;
				}else{	
					currentCursor = btree_cursor;
				}
				status = currentCursor.getFirst(key, data,LockMode.DEFAULT);
				while (status == OperationStatus.SUCCESS){
					if (sameData(userInputDataString,data)){
				    	 writeToFile(key,data);
				    	 numberOfRecordRetrived++;
					}
				    data.setData(null);
				    key.setData(null);
					status = currentCursor.getNext(key, data,LockMode.DEFAULT);
				}
		    }else{
			  //============== If index selected ==============
				currentCursor = index_cursor;
		    	key.setData(userInputDataString.getBytes());
		    	status = currentCursor.getSearchKey(key,data,LockMode.DEFAULT);
		    	if (status==OperationStatus.SUCCESS){
			    	 numberOfRecordRetrived++;
			    	 writeToFile(data,key); //reverse key and data because it is from the index table
		    	}
		    }
		    
			endTime =  System.nanoTime();
			durationInMicroSecond = (endTime - startTime)/1000;
		    System.out.println("Retrived "+numberOfRecordRetrived+" record.");
		    System.out.println("Took: "+durationInMicroSecond+" micro second");
	    	

		} catch (DatabaseException e) {
			System.err.println("retriveRecordsByData failed: " + e.toString());
			System.exit(-1);
		}	
	}

	/*
	 * Author: Jim
	 */
	public void retriveRecordsByRange() {
		
		try {
			Cursor currentCursor;
			DatabaseEntry key = new DatabaseEntry();
		    DatabaseEntry data = new DatabaseEntry();	 
		    Scanner scanner = new Scanner(System.in);
		    System.out.println("Please enter the lower bound: ");
		    String lowerBound = scanner.nextLine();
		    System.out.println("Please enter the upper bound: ");
		    String upperBound = scanner.nextLine();
				    
		    int numberOfRecordRetrived = 0;
		    long startTime;
		    long endTime;
		    long durationInMicroSecond;
		    startTime =  System.nanoTime();
		    
		    OperationStatus status;
		    //============== If hash selected ==============
			if (databaseTypeFromUser.equals("hash")){						
				currentCursor = hash_cursor;
				status= currentCursor.getFirst(key, data,LockMode.DEFAULT);
				
			    //setting cursor to lower bound
			    while (status == OperationStatus.SUCCESS){
			    	if (keyInsideRange(key,lowerBound,upperBound)){
						writeToFile(key,data);
					    numberOfRecordRetrived++;
			    	}
				    data.setData(null);
				    key.setData(null);
					status = currentCursor.getNext(key, data,LockMode.DEFAULT);
			    }
			}else{		
				 //============== If btree or index selected ==============
				currentCursor = btree_cursor;
				key.setData(lowerBound.getBytes());
				key.setSize(lowerBound.length());
				status = currentCursor.getSearchKeyRange(key,data,LockMode.DEFAULT);
				
			    //Once the cursor is at the lower bound
				while (status == OperationStatus.SUCCESS){
				    if (keyInsideRange(key,lowerBound,upperBound)){
						writeToFile(key,data);
					    numberOfRecordRetrived++;
				    }else{
				    	break;
				    }
				    data.setData(null);
				    key.setData(null);
					status = currentCursor.getNext(key, data,LockMode.DEFAULT);
				}
			}
			endTime =  System.nanoTime();
			durationInMicroSecond = (endTime - startTime)/1000;
	    	System.out.println("Retrived "+numberOfRecordRetrived+" record.");
	    	System.out.println("Took: "+durationInMicroSecond+" micro second");
		} catch (DatabaseException e) {
			System.err.println("retriveRecordsByData failed: " + e.toString());
			System.exit(-1);
		}
	}

	/*
	 * Author: Nelson 
	 * Closes the database and then removes it from disk as well
	 */
	public void destoryDatabase() {

		try {
			hash_table.close();
			btree_table.close();
			index_table.close();
		
			//remove all previous data before starting doing anything
			try{
				Database.remove(HASH_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}
			try{
				Database.remove(BTREE_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}
			try{
				Database.remove(INDEX_TABLE, null, null);
			}catch(FileNotFoundException e){
				//file already delelted. move on
			}

			System.out.println("Database closed and destroyed succesfully.");
			return;

		} catch (Exception e1) {
			System.err.println("destroyDatabase failed: " + e1.toString());
		}
	}

	// -------------------------HELPER FUNCTIONS----------------------//
	private boolean sameData(String stringValue1, DatabaseEntry data){
		String stringValue2 = new String (data.getData());
		return stringValue1.equals(stringValue2);
	}

	private boolean keyInsideRange(DatabaseEntry key, String lowerBound, String upperBound){
		String keyString = new String (key.getData());
		return (keyString.compareTo(lowerBound)>=0) && (keyString.compareTo(upperBound)<=0);
	}
	
	private void writeToFile(DatabaseEntry key, DatabaseEntry data) {
		try {
			String filename = "answers.txt";
			String keyString = new String(key.getData());
			String dataString = new String(data.getData());
			
			//true will append the new data to the end of the file
			FileWriter fw = new FileWriter(filename, true);
			fw.write(keyString + "\n");
			fw.write(dataString + "\n");
			fw.write("\n");
			fw.close();
		} catch (IOException ioe) {
			System.err.println("IOException: " + ioe.getMessage());
		}
	}

	/*
	 * To pouplate the given table with nrecs records
	 */
	private void populateTable(int nrecs) {

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
				status = hash_table.putNoOverwrite(null, kdbt, ddbt);
				status = btree_table.putNoOverwrite(null, kdbt, ddbt);
				//create the index table by reversing the key and data values
				status = index_table.putNoOverwrite(null, ddbt, kdbt);

				if (status.toString().equalsIgnoreCase(
						"OperationStatus.KEYEXIST")) {
					//System.out.println("Key already exists, skipping this key value pair");
				}
			}
		} catch (DatabaseException dbe) {
			System.err.println("Populate the table: " + dbe.toString());
			System.exit(1);
		}
	}

}

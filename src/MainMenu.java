import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Scanner;

//Author: Nelson
public class MainMenu {

	private static int quit_code = 6;
	private static int smallest_value = 1;
	private static int largest_value = 6;

	//
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		int menu_choice = -1;

		// input validation
		if (args.length == 0) {
			System.out.println("No input arguments, please enter a valid input argument.");
			return;
		}

		if ((args[0].equals("btree")) || (args[0].equals("hash"))
				|| (args[0].equals("indexfile"))) {
			// System.out.println("Good input");
		} else {
			System.out.println("Incorrect input. Must be: 'btree' or 'hash' or 'indexfile'");
			return;
		}

		BerkleyDBClient berkleyDBClient= new BerkleyDBClient (args[0]);
		
		//erase the contents of the file
		try {
			PrintWriter writer = new PrintWriter("answers.txt");
			writer.print("");
			writer.close();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		};		

		//start the menu
		while (menu_choice != quit_code) {
			menu_choice = get_menu_input();
			if (menu_choice != quit_code) {
				switch (menu_choice) {
				case 1:
					berkleyDBClient.populate();
					break;
				case 2:
					berkleyDBClient.retriveRecordsByKey();
					break;
				case 3:
					berkleyDBClient.retriveRecordsByData();
					break;
				case 4:
					berkleyDBClient.retriveRecordsByRange();
					break;
				case 5:
					berkleyDBClient.destoryDatabase();
				}

			}

			// delay until the user press enter to display the menu again
			if (menu_choice != quit_code) {
				System.out.println("Press ENTER to go back to the main menu.");
				try {
					System.in.read();
				} catch (IOException e) {
					// TODO Auto-generated catch block
				}
			}

		}

		//berkleyDBClient.destoryDatabase();
		System.out.println("Closing database and exiting. Bye!");
	}

	public static int get_menu_input() {

		Scanner user_input = new Scanner(System.in);
		int user_option = -1;

		while (user_option < 1 || user_option > 6) {
			System.out.println("MAIN MENU");
			System.out.println("Please choose from the following options:");
			System.out.println("1: Create and Populate a database");
			System.out.println("2: Retrieve records with a given key");
			System.out.println("3: Retrieve records with a given data");
			System.out
					.println("4: Retrieve records with a given range of key values");
			System.out.println("5: Destroy the database");
			System.out.println("6: Quit");

			if (!user_input.hasNextInt()) {
				System.out.print("ERROR: ");
				System.out.println("Not an integer. Please enter an integer");
				System.out.println();
				user_input.next();
				continue;
			}

			user_option = user_input.nextInt();
			if (user_option > largest_value || user_option < smallest_value) {
				System.out.print("ERROR: ");
				System.out.println("Not a valid menu option. Please try again");
				System.out.println();
			}
		}

		return user_option;
	}
}

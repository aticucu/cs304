// We need to import the java.sql package to use JDBC
import java.sql.*;
// for reading from the command line
import java.io.*;

/*
 * This class implements a graphical login window and a simple text
 * interface for interacting with the branch table 
 */ 
public class asg3
{
	// command line reader 
	private BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

	private Connection con;

	/*
	 * constructs login window and loads JDBC driver
	 */ 
	public asg3()
	{
		String username = null;
		String password = null;
		
		// Get username and password
		try {
			System.out.print("Username: ");
			username = in.readLine();
			System.out.print("Password: ");
			password = in.readLine();
		} catch (IOException e) {
			System.out.println("I/O exception on login: " + e.getMessage());
		}
		
		// Load the Oracle JDBC driver
		try 
		{
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());
			System.exit(-1);
		}
		
		// Connect to ubc oracle db
		if (username != null && password != null) {
			connect(username, password);
		}
		else {
			System.out.println("Empty username or password");
			System.exit(-1);
		}
		
		showMenu();
	}


	/*
	 * connects to Oracle database named ug using user supplied username and password
	 */ 
	private boolean connect(String username, String password)
	{
		String connectURL = "jdbc:oracle:thin:@dbhost.ugrad.cs.ubc.ca:1522:ug"; 

		try 
		{
			con = DriverManager.getConnection(connectURL,username,password);

			System.out.println("\nConnected to Oracle!");
			return true;
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());
			return false;
		}
	}

	/*
	 * displays simple text interface
	 */ 
	private void showMenu()
	{
		int choice;
		boolean quit;

		quit = false;

		try 
		{
			// disable auto commit mode
			con.setAutoCommit(false);

			while (!quit)
			{
				System.out.print("\n\nPlease choose one of the following: \n");
				System.out.print("1.  Part2 Insert item\n");
				System.out.print("2.  Part2 Remove item\n");
				System.out.print("3.  Part3\n");
				System.out.print("4.  Part4\n");
				System.out.print("5.  Quit\n>> ");

				choice = Integer.parseInt(in.readLine());

				System.out.println(" ");

				switch(choice)
				{
				case 1:  insertItem(); break;
				case 2:  deleteItem(); break;
				case 3:  showTextbooks(); break;
				case 4:  showTop3Items(); break;
				case 5:  quit = true;
				}
			}

			con.close();
			in.close();
			System.out.println("\nGood Bye!\n\n");
			System.exit(0);
		}
		catch (IOException e)
		{
			System.out.println("IOException!");

			try
			{
				con.close();
				System.exit(-1);
			}
			catch (SQLException ex)
			{
				System.out.println("Message: " + ex.getMessage());
			}
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());
		}
	}

	/*
	 * Part 1) Enter a new item
	 */
	private void insertItem() 
	{
		String				upc;
		String				bookFlag;
		
		try {
			System.out.print("\nItem upc: ");
			upc = in.readLine();
			
			// Check itemPurchase table for the existing upc and delete if upc present
			if(checkExistingItem(upc)) {
				System.out.println("Item with the given upc already exists...");
			}
			else {
				// Insert the item
				insertToItem(upc);
				System.out.print("\nIs this a book? (y or n): ");
				bookFlag = in.readLine();
				
				// Insert item to book table as well if the item is a book
				if(bookFlag.equals("y")) {
					insertToBook(upc);
				}
			
				// commit work 
				con.commit();
			}
		}
		catch (SQLException ex) {
			System.out.println("Message: " + ex.getMessage());
			try 
			{
				// undo the insert
				con.rollback();	
			}
			catch (SQLException ex2)
			{
				System.out.println("Message: " + ex2.getMessage());
				System.exit(-1);
			}
		} 
		catch (IOException e) {
			// Some error occurred while user input
			e.printStackTrace();
		}
	}


	private void insertToItem(String upc) throws SQLException, IOException {
		float sellingPrice;
		int stock;
		String taxable;
		PreparedStatement ps;
		
		ps = con.prepareStatement("INSERT INTO item VALUES (?,?,?,?)");
		
		System.out.print("\nItem Selling Price: ");
		sellingPrice = Float.parseFloat(in.readLine());
		while(sellingPrice < 0) {
			System.out.print("\nItem selling price cannot be a negative value! Please insert again: ");
			sellingPrice = Float.parseFloat(in.readLine());
		}
		
		System.out.print("\nItem stock: ");
		stock = Integer.parseInt(in.readLine());
		while(stock < 0) {
			System.out.print("\nItem stock cannot be a negative value! Please insert again: ");
			stock = Integer.parseInt(in.readLine());
		}
		
		System.out.print("\nItem taxable?: ");
		taxable = in.readLine();
		while(!(taxable.equals("y") || taxable.equals("n"))) {
			System.out.print("\nTaxable field can only be y or n! Please insert again: ");
			taxable = in.readLine();
		}
		
		ps.setString(1, upc);
		ps.setFloat(2, sellingPrice);
		ps.setInt(3, stock);
		ps.setString(4, taxable);
		
		ps.executeUpdate();
		ps.close();
	}


	private void insertToBook(String upc) throws SQLException, IOException {
		String title;
		String publisher;
		String textbookFlag;
		PreparedStatement psBook;
		
		psBook = con.prepareStatement("INSERT INTO book VALUES (?,?,?,?)");
		
		System.out.print("\nBook title?: ");
		title = in.readLine();
		
		System.out.print("\nBook publisher?: ");
		publisher = in.readLine();
		
		System.out.print("\nIs the book textbook? (y or n): ");
		textbookFlag = in.readLine();
		
		psBook.setString(1, upc);
		psBook.setString(2, title);
		psBook.setString(3, publisher);
		psBook.setString(4, textbookFlag);
		
		psBook.executeUpdate();
		psBook.close();
	}


	private boolean checkExistingItem(String upc) throws SQLException {
		PreparedStatement ps;
		ps = con.prepareStatement("SELECT * FROM item WHERE upc = ?");
		
		ps.setString(1, upc);
		int rowCount = ps.executeUpdate();
		ps.close();
		if(rowCount > 0) {
			return true;
		}
		return false;
	}

	/*
	 * deletes an item
	 */ 
	private void deleteItem()
	{
		String                	upc;
		PreparedStatement  		ps;
		try
		{
			System.out.print("\nItem upc: ");
			upc = in.readLine();

			if(checkStock(upc)) {
				// Check itemPurchase table for the existing upc and delete if upc present
				if(checkTable(upc, "itemPurchase")) {
					deleteFromTable(upc, "itemPurchase");
				}

				// Check Book table for the existing upc and delete if upc present
				if(checkTable(upc, "book")) {
					deleteFromTable(upc, "book");
				}

				// Delete item after checking for child constraints
				ps = con.prepareStatement("DELETE FROM item WHERE upc = ? AND stock = 0");
				ps.setString(1, upc);
				ps.executeUpdate();
				ps.close();
				System.out.println("Successfully deleted " + upc + " from item table.");
			}
			else
			{
				System.out.println("\nTransaction cancelled because : Item " + upc + " does not exist! or the stock is not at 0");
			}
			con.commit();
		}
		catch (IOException e)
		{
			System.out.println("IOException!");
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());

			try 
			{
				con.rollback();	
			}
			catch (SQLException ex2)
			{
				System.out.println("Message: " + ex2.getMessage());
				System.exit(-1);
			}
		}
	}


	private void deleteFromTable(String upc, String table) throws SQLException {
		PreparedStatement deleteCascadePs = null;
		if(table.equals("itemPurchase")){
			deleteCascadePs = con.prepareStatement("DELETE FROM itemPurchase WHERE upc = ?");
		}
		if(table.equals("book")){
			deleteCascadePs = con.prepareStatement("DELETE FROM book WHERE upc = ?");
		}
		
		if(deleteCascadePs != null) {
			deleteCascadePs.setString(1, upc);
			deleteCascadePs.executeUpdate();
		}
		else {
			System.out.println("Table could not be recognized when deleting item in the given table: " + table);
			return;
		}
		deleteCascadePs.close();
		System.out.println("Successfully deleted " + upc + " from " + table + " table.");
	}


	private boolean checkTable(String upc, String table) throws SQLException {
		int cCount;
		PreparedStatement checkPs = null;
		
		if(table.equals("itemPurchase")) {
			checkPs = con.prepareStatement("SELECT * FROM itemPurchase WHERE upc = ?");
			checkPs.setString(1, upc);
		}
		
		if(table.equals("book")) {
			checkPs = con.prepareStatement("SELECT * FROM book WHERE upc = ?");
			checkPs.setString(1, upc);
		}
		
		if(checkPs == null) {
			System.out.println("Table could not be recognized when checking item in the given table: " + table);
			return false;
		}
		cCount = checkPs.executeUpdate();
		checkPs.close();
		if (cCount > 0) {
			return true;
		}
		return false;
	}


	private boolean checkStock(String upc) throws SQLException {
		int cCount;
		PreparedStatement checkPs;
		
		checkPs = con.prepareStatement("SELECT * FROM item WHERE upc = ? AND stock = 0");
		
		checkPs.setString(1, upc);
		cCount = checkPs.executeUpdate();
		checkPs.close();
		
		if (cCount > 0)
			return true;
		
		return false;
	}

	
	/*
	 * Part 3
	 */ 
	private void showTextbooks()
	{
		Statement stmt;
		ResultSet rs;
		try
		{
			stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT ip.upc FROM item i, book b, purchase p, itemPurchase ip WHERE " +
					"i.upc = b.upc AND b.upc = ip.upc AND ip.t_id = p.t_id AND i.stock < 10 AND " +
					"p.purchaseDate >= '15-OCT-25' AND p.purchaseDate <= '15-OCT-31' AND b.flag_text = 'y'" +
					"GROUP BY ip.upc HAVING SUM(ip.quantity) >= 50");

			while(rs.next())
			{
				System.out.println("UPC: " + rs.getString("upc"));
			}

			con.commit();

			rs.close();
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());

			try 
			{
				con.rollback();	
			}
			catch (SQLException ex2)
			{
				System.out.println("Message: " + ex2.getMessage());
				System.exit(-1);
			}
		}	
	}
	
	/*
	 * Part 4
	 */ 
	private void showTop3Items()
	{
		Statement stmt;
		ResultSet rs;
		try
		{
			stmt = con.createStatement();
			stmt.executeQuery("DROP VIEW itemRecord");
			stmt.close();
			
			stmt = con.createStatement();
			stmt.executeQuery("CREATE VIEW itemRecord AS " +
								"SELECT ip.upc, SUM(ip.quantity) as sum FROM purchase p, item i, itemPurchase ip WHERE " +
								"p.purchaseDate >= '15-OCT-25' AND p.purchaseDate <= '15-OCT-31' AND " +
								"ip.t_id = p.t_id AND ip.upc = i.upc GROUP BY ip.upc");
			stmt.close();
			stmt = con.createStatement();
			rs = stmt.executeQuery("SELECT * FROM (SELECT i.upc, ir.sum, i.sellingPrice " + 
									"FROM item i, itemRecord ir " +
									"WHERE i.upc = ir.upc " + 
									"ORDER BY (i.sellingPrice * ir.sum) DESC) WHERE ROWNUM <= 3");
			while(rs.next())
			{
				System.out.println("upc: " + rs.getString(1) + " sum: " + rs.getString(2)+ " price: " + rs.getString(3));
			}

			con.commit();

			rs.close();
		}
		catch (SQLException ex)
		{
			System.out.println("Message: " + ex.getMessage());

			try 
			{
				con.rollback();	
			}
			catch (SQLException ex2)
			{
				System.out.println("Message: " + ex2.getMessage());
				System.exit(-1);
			}
		}	
	}


	public static void main(String args[])
	{
		new asg3();
	}
}

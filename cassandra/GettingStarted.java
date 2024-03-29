package cassandra;

import com.datastax.driver.core.*;


@SuppressWarnings("javadoc")
public class GettingStarted {

	public static void main(String[] args) {

		Cluster cluster;
		Session session;

		// Connect to the cluster and keyspace "demo"
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		// System.out.println("printing cluster "+cluster);
	
		
		session = cluster.connect();
		session.execute("CREATE KEYSPACE IF NOT EXISTS demo WITH replication = {"
				  + " 'class': 'SimpleStrategy', "
				  + " 'replication_factor': '3' "
				  + "};" );
		session.execute("USE demo");
	
		session.execute("create table if not exists users (lastname text, age int, city text, email text, firstname text, PRIMARY KEY (lastname))");

		// Insert one record into the users table
		session.execute("INSERT INTO users (lastname, age, city, email, firstname) VALUES ('Jones', 35, 'Austin', 'bob@example.com', 'Bob')");

		// readTest(session);

		// Use select to get the user we just entered
		ResultSet results = session
				.execute("SELECT * FROM users WHERE lastname='Jones'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"),
					row.getInt("age"));
		}

		// Update the same user with a new age
		session.execute("update users set age = 36 where lastname = 'Jones'");

		// Select and show the change
		results = session.execute("select * from users where lastname='Jones'");
		for (Row row : results) {
			System.out.format("%s %d\n", row.getString("firstname"),
					row.getInt("age"));
		}

		// Delete the user from the users table
		session.execute("DELETE FROM users WHERE lastname = 'Jones'");

		// Show that the user is gone
		results = session.execute("SELECT * FROM users");
		for (Row row : results) {
			System.out.format("%s %d %s %s %s\n", row.getString("lastname"),
					row.getInt("age"), row.getString("city"),
					row.getString("email"), row.getString("firstname"));
		}

		session.execute("drop table users");
		// Clean up the connection by closing it
		cluster.close();
	}
}

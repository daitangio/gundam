package com.nttdata.gundam;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.siforge.sm.SmartSync;
public class SmartSyncExample1 {

	private Logger   logger=Logger.getLogger(getClass());
	public static void main(String[] args) throws ClassNotFoundException
	{
		BasicConfigurator.configure();
		// load the sqlite-JDBC driver using the current class loader
		Class.forName("org.sqlite.JDBC");
		(new SmartSyncExample1()).playDemo();

	}

	private void playDemo() {
		Connection connection = null,db2;
		try
		{
			// create a database connection. Exmple of in memory
			connection = DriverManager.getConnection("jdbc:sqlite:./db1.sqlite");
			db2=DriverManager.getConnection("jdbc:sqlite:./db2.sqlite");
			populateDummyData(connection);
			populateDummyData(db2);
			db2.prepareStatement("DELETE FROM PERSON").execute();
			logger.info("Db1 and db2 ready. Demo sync db1->db2");
			SmartSync s1=new SmartSync("PERSON",connection,db2,logger);
			s1.syncAll();
		}
		catch(SQLException e)
		{
			// if the error message is "out of memory", 
			// it probably means no database file is found
			System.err.println(e.getMessage());
		}
		finally
		{
			try
			{
				if(connection != null)
					connection.close();
			}
			catch(SQLException e)
			{
				// connection close failed.
				System.err.println(e);
			}
		}

		
	}

	public void populateDummyData(Connection connection)
			throws SQLException {
		Statement statement = connection.createStatement();
		statement.setQueryTimeout(30);  // set timeout to 30 sec.

		statement.executeUpdate("drop table if exists person");
		statement.executeUpdate("create table person (id integer, name string)");
		statement.executeUpdate("insert into person values(1, 'leo')");
		statement.executeUpdate("insert into person values(2, 'yui')");
		ResultSet rs = statement.executeQuery("select * from person");
		while(rs.next())
		{
			// read the result set
			System.out.println("name = " + rs.getString("name"));
			System.out.println("id = " + rs.getInt("id"));
		}
	}
}

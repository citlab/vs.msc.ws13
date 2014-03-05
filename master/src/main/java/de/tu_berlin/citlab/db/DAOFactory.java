package de.tu_berlin.citlab.db;

public class DAOFactory
{
	public static DAO createDAO( String type )
	{
		if ( type.equalsIgnoreCase( "Cassandra" ) )
		{
			return new CassandraDAO();
		}
/*		else if ( type.equalsIgnoreCase( "MySQL" ) )
		{
			return new MySQLDAO();
		}*/
		
		return null;
	}

}

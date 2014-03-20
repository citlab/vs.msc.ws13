package de.tu_berlin.citlab.db;

import java.util.List;
import java.util.ArrayList;
import backtype.storm.tuple.Values;


public class TestSelect
{
	public static void main( String[] args ) throws Exception
	{
		//Vorgabe:
		//List<Values> = CassandraDAO.source("citstorm", "user_significance").findAll()
		//List<Values> = CassandraDAO.source("citstorm", "user_significance").findBy( KeyFields, Values )
		
		CassandraDAO dao = new CassandraDAO();
		dao.connect( "127.0.0.1" );
		
		List<Values> listOfValues = new ArrayList<Values>();
		
		//listOfValues = dao.source( "myks", "tab1" ).findBy( "user", "Jules" );
		listOfValues = dao.source( "myks", "tab1" ).findAll();
		
		System.out.println(listOfValues);
		
		dao.shutdown();
		

		

	}

}

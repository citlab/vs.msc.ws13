package de.tu_berlin.citlab.db;

import de.tu_berlin.citlab.storm.operators.CassandraOperator;
import backtype.storm.tuple.Fields;

public class Test
{

	public static void main( String[] args )
	{
		CassandraConfig cassandraCfg = new CassandraConfig();

		cassandraCfg.setParams( // optional, but defaults not always sensable
				"myks",
				"new2", 
				new PrimaryKey( "user", "id" ), 
				new Fields() 
		);
		CassandraOperator co = new CassandraOperator(cassandraCfg);

	}

}

package de.tu_berlin.citlab.db;

import java.util.Iterator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import backtype.storm.tuple.Values;

// An iterator used for retrieving values from result sets with Cassandra data types
public class CassandraIterator implements Iterator<Values> { 
	    private Iterator<Row> rows;
	    public CassandraIterator( ResultSet rs )
	    {
	    	this.rows = rs.iterator();
	    }

		@Override
		public boolean hasNext() {
			return rows.hasNext();
		}

		@Override
		public Values next() {
			return CassandraDAO.getValuesFromRow(rows.next());
		}

		@Override
		public void remove() {
			rows.remove();
		}
	     


}



package de.tu_berlin.citlab.db;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;

import backtype.storm.tuple.Values;

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



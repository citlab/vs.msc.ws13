package de.tu_berlin.citlab.storm.helpers;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.IKeyConfig;
import de.tu_berlin.citlab.storm.window.TupleComparator;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@SuppressWarnings("serial")
public class KeyConfigFactory implements Serializable {
	
	private static IKeyConfig defaultKey = new IKeyConfig() {
		private Serializable defaultKey = 0;
		public Serializable getKeyOf(Tuple input) {
			return defaultKey;
		}
	};
	
	private static IKeyConfig bySource = new IKeyConfig() {
		public Serializable getKeyOf(Tuple input) {
			return input.getSourceComponent();
		}
	};
	
	public static IKeyConfig ByFields(final String... fields) {
		return ByFields(new Fields(fields));
	}
	
	public static IKeyConfig ByFields(final Fields keyFields) {
		return new IKeyConfig() {
			public Serializable getKeyOf(Tuple input) {
				return (Serializable)input.select(keyFields);
			}
		};
	}
	// org.apache.commons.lang.StringUtils.join( 
	
	public static IKeyConfig BySource() {
		return bySource;
	}
	
	public static IKeyConfig DefaultKey() {
		return defaultKey;
	}
	
	public static TupleComparator compareByFields(final Fields keyFields) {
		return new TupleComparator () {
		    @SuppressWarnings({ "unchecked", "rawtypes" })
			public int compare(Tuple first, Tuple second) {
		    	  if(keyFields.size() == 1 ){
		    		  Comparable left = (Comparable)first.getValueByField(keyFields.get(0));
		    		  Comparable right =(Comparable)second.getValueByField(keyFields.get(0));
		    		  return left.compareTo(right);
		    		  
		    	  } else {
		    		  int counter=1;
		    		  int total_result=0;
		    		  for( String key : keyFields ){
		    			 int result = ((Comparable)first.getValueByField(key)) .compareTo( (Comparable)first.getValueByField(key));
		    			 if(result != 0 ){
		    				 total_result= counter * result;
		    				 break;
		    			 }
		    		  }//for
		    		  return total_result;
		    	  }//if
		      } // compare()
			@Override
			public Serializable getTupleKey(Tuple tuple) {
				return (Serializable) tuple.select(keyFields);
			}
		};
	}	

}

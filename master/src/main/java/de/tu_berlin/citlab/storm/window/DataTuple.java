package de.tu_berlin.citlab.storm.window;

import java.util.List;

import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DataTuple {
	private Values values;
	private Fields fields;
	
	public DataTuple(){
		values = new Values();
		fields = new Fields();
	}
	public DataTuple(Values v, Fields f){
		values = v;
		fields = f;
	}
	
	public Values getValues(){
		return values;
	}
	public Fields getFields(){
		return fields;
	}
	
	public String toString(){
		String str="("; //+values.toString()+";"+fields.toString();
		for(int i=0; i < values.size(); i++ ){
			str+=fields.get(i)+":"+values.get(i).toString()+";";
		}
		str+=")";
		return str;
	}
	public Object get(String field){
		if(fields.contains(field)){
			return values.get(fields.fieldIndex(field) );
		}else {
			return null;
		}
	}
	
	public void set(String field, Object value){
		if(fields.contains(field)){
			values.set(fields.fieldIndex(field), value);
		}else {
			List<String> fnew = fields.toList();
			fnew.add(field);
			fields = new Fields(fnew);
			values.add(value);
			values.set(fields.fieldIndex(field), value);
		}
	}
}

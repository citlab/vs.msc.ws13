package de.tu_berlin.citlab.storm.udf;

public class Context {
	private String source;
	
	public Context( String source){
		this.source = source;
	}
	
	public String getSource() {
		return source;
	}
}

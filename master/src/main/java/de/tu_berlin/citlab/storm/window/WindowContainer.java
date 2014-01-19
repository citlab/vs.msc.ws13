package de.tu_berlin.citlab.storm.window;

import java.util.List;
import java.io.Serializable;

public class WindowContainer<I> implements Serializable, Cloneable 
{
	private static final long serialVersionUID = 1L;
		
	private final List<I> window;
	
	public WindowContainer(List<I> w){
		this.window = w;
	}
	
	public List<I> getWindow(){
		return window;
	}
	
	public void sort(  ){
		
	}
}
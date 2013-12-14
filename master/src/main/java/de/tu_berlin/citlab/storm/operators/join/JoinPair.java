package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import de.tu_berlin.citlab.storm.window.DataTuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinPair implements Serializable  {
	private static final long serialVersionUID = -2170248605926103695L;	
	private WindowContainer<DataTuple> inner;
	private WindowContainer<DataTuple> outer;
	public JoinPair(WindowContainer<DataTuple> i, WindowContainer<DataTuple> o){
		inner=i;
		outer=o;
	}
	public WindowContainer<DataTuple> getInner(){
		return inner;
	}
	public WindowContainer<DataTuple> getOuter(){
		return outer;
	}
	
}

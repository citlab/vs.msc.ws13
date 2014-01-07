package de.tu_berlin.citlab.storm.operators.join;

import java.io.Serializable;

import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.window.WindowContainer;

public class JoinPair implements Serializable  {
	private static final long serialVersionUID = -2170248605926103695L;	
	private WindowContainer<Tuple> inner;
	private WindowContainer<Tuple> outer;
	public JoinPair(WindowContainer<Tuple> i, WindowContainer<Tuple> o){
		inner=i;
		outer=o;
	}
	public WindowContainer<Tuple> getInner(){
		return inner;
	}
	public WindowContainer<Tuple> getOuter(){
		return outer;
	}
	
}

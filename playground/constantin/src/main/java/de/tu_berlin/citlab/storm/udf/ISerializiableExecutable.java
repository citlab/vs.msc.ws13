package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;

public interface ISerializiableExecutable<I, O> extends Serializable {
	
	public O execute(I param); 

}

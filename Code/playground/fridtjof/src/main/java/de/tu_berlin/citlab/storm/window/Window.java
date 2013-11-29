package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;

public interface Window<I, O> extends Serializable, Cloneable {

	public void add(I input);
	
	public boolean isSatisfied();

	public O flush();

	public Window<I, O> clone();

}

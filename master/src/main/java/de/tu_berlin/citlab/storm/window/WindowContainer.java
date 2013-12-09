package de.tu_berlin.citlab.storm.window;

import java.io.Serializable;

public class WindowContainer<K, I, O> implements Serializable, Cloneable 
{
	private static final long serialVersionUID = 1L;
	
	
	public final K key;
	public final Window<I, O> window;

	public WindowContainer(K key, Window<I, O> window)
	{
		this.key = key;
		this.window = window;
	}
}

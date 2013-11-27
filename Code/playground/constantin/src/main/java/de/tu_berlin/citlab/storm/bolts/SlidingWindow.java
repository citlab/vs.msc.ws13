package de.tu_berlin.citlab.storm.bolts;


import java.util.ArrayList;
import java.util.Iterator;


public class SlidingWindow<WindowEntry> 
{
	private final ArrayList<WindowEntry> _windowEntries;
	
	private final long _windowSize;
	
	public SlidingWindow(long windowSize)
	{
		_windowEntries = new ArrayList<WindowEntry>();
		_windowSize = windowSize;
	}
	
	
	public boolean appendEntry(WindowEntry entry)
	{
		boolean added = _windowEntries.add(entry);
		return added;
	}

	
	public boolean check_sizeReached()
	{
		if(_windowEntries.size() < _windowSize)
			return false;
		else return true;
	}
	
	//TODO: reset Sliding Window after window was successfully submitted.
	
	public Iterator<WindowEntry> getWindowEntries()
	{
		return _windowEntries.iterator();
	}
}

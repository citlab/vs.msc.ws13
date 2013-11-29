package de.tu_berlin.citlab.storm.bolts;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

public class BucketStore<Key extends Comparable<Key>, WindowEntry> implements Serializable
{
	private static final long serialVersionUID = 1L;
	
	
/* Global Constants: */
/* ================= */
	
	private final Vector<SlidingWindow<WindowEntry>> _bucketStore;
	private final HashMap<Key, WindowPointer> _bucketPointer;
	private final Vector<WindowPointer> _fullWindows;
	private final long _windowSize;
	
	
/* Constructor: */
/* ============ */
	
	public BucketStore(long windowSize)
	{
		//Generic Workaround for "new SlidingWindow<BucketData>>[FieldKeys.values().length];":
		//_bucketStore = (SlidingWindow<WindowEntry>[]) Array.newInstance(SlidingWindow.class, FieldKeys.values().length);
		_bucketStore = new Vector<SlidingWindow<WindowEntry>>();
		_bucketPointer = new HashMap<Key, WindowPointer>();
		_fullWindows = new Vector<WindowPointer>();
		_windowSize = windowSize;
	}

	
	
/* Public-Methods: */
/* =============== */

	public SlidingWindow<WindowEntry> sortInBucket(Key sortKey, WindowEntry input)
	{
		SlidingWindow<WindowEntry> slidingWindow;
		if(_bucketPointer.containsKey(sortKey))
		{
			WindowPointer winPtr = _bucketPointer.get(sortKey);
			slidingWindow = _bucketStore.get(winPtr.getVectorIndex());
			slidingWindow.appendEntry(input);
			winPtr.setWinSizeReached(slidingWindow.check_sizeReached());
			if(winPtr.isWindowSizeReached())
				_fullWindows.add(winPtr);
		}
		else
		{
			slidingWindow = new SlidingWindow<WindowEntry>(_windowSize);
			slidingWindow.appendEntry(input);
			boolean added = _bucketStore.add(slidingWindow);
			if(added)
			{
				int vectorIndex = _bucketStore.indexOf(slidingWindow);
				boolean sizeReached = slidingWindow.check_sizeReached();
				WindowPointer winPtr = new WindowPointer(sortKey, vectorIndex, sizeReached);
				_bucketPointer.put(sortKey, winPtr);
				if(winPtr.isWindowSizeReached())
					_fullWindows.add(winPtr);
			}
				
		}
		return slidingWindow;
	}
	
	
	public HashMap<Key, Iterator<WindowEntry>> readyForExecution()
	{
		HashMap<Key, Iterator<WindowEntry>> readyEntryGroups = new HashMap<Key, Iterator<WindowEntry>>();
		for(WindowPointer actWinPtr : _fullWindows)
		{
			Iterator<WindowEntry> entryIt = _bucketStore.get(actWinPtr.getVectorIndex()).getWindowEntries();
			readyEntryGroups.put(actWinPtr.getSortKey(), entryIt);
		}
		
		return readyEntryGroups;
	}
	
	
	
	
/* Private internal Class: */
/* ======================= */
	
	private class WindowPointer implements Serializable
	{
		private static final long serialVersionUID = 1L;
		
		private final Key _sortKey;
		private final int _vectorIndex;
		private boolean _winSizeReached;
		
		public WindowPointer(Key sortKey, int vectorIndex, boolean winSizeReached)
		{
			_sortKey = sortKey;
			_vectorIndex = vectorIndex;
			_winSizeReached = winSizeReached;
		}

		public final Key getSortKey() {
			return _sortKey;
		}

		public final int getVectorIndex() {
			return _vectorIndex;
		}

		public void setWinSizeReached(boolean winSizeReached){
			_winSizeReached = winSizeReached;
		}
		public final boolean isWindowSizeReached() {
			return _winSizeReached;
		}
		
		
	}
}

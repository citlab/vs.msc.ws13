package stormCG.bolts.windows;


import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Vector;


public class BucketStore<Key /*extends Comparable<Key>*/, WindowEntry> implements Serializable
{
	private static final long serialVersionUID = 1L;

/* Public Enums: */
/* ============= */
	
	public enum WinTypes {None, CounterBased, TimeBased}
	
	
/* Global Constants: */
/* ================= */
	
	private final Vector<SlidingWindow<WindowEntry>> _bucketStore;
	private final HashMap<Key, WindowPointer> _bucketPointer;
	private final Vector<WindowPointer> _fullWindows;
	private final int _windowSize;
	private final WinTypes _winType;
	
	
/* Constructor: */
/* ============ */
	
	public BucketStore()
	{
		this(1, WinTypes.None);
	}
	
	public BucketStore(int windowSize, WinTypes winType)
	{
		_winType = winType;
		_bucketStore = new Vector<SlidingWindow<WindowEntry>>();
		_bucketPointer = new HashMap<Key, WindowPointer>();
		_fullWindows = new Vector<WindowPointer>();
		_windowSize = windowSize;
	}

	
	
/* Public-Methods: */
/* =============== */

	public SlidingWindow<WindowEntry> sortInBucket(Key sortKey, WindowEntry input)
										throws NullPointerException
	{
		SlidingWindow<WindowEntry> slidingWindow;
		if(_bucketPointer.containsKey(sortKey))
		{
			WindowPointer winPtr = _bucketPointer.get(sortKey);
			slidingWindow = _bucketStore.get(winPtr.getVectorIndex());
			slidingWindow.appendEntry(input);
			winPtr.setWinSizeReached(slidingWindow.check_windowLimit());
			if(winPtr.isWindowSizeReached())
				_fullWindows.add(winPtr);
		}
		else
		{
			switch(_winType)
			{
				case CounterBased:
					slidingWindow = new CountWindow<WindowEntry>(_windowSize);
					break;
				case TimeBased:
					slidingWindow = new TimeWindow<WindowEntry>(_windowSize);
					break;
				default:
					slidingWindow = null;
			}
			if(slidingWindow != null)
			{
				slidingWindow.appendEntry(input);
				boolean added = _bucketStore.add(slidingWindow);
				if(added)
				{
					int vectorIndex = _bucketStore.indexOf(slidingWindow);
					boolean sizeReached = slidingWindow.check_windowLimit();
					WindowPointer winPtr = new WindowPointer(sortKey, vectorIndex, sizeReached);
					_bucketPointer.put(sortKey, winPtr);
					if(winPtr.isWindowSizeReached())
						_fullWindows.add(winPtr);
				}
			}
			else throw new NullPointerException();
				
		}
		return slidingWindow;
	}
	
	
	public HashMap<Key, List<WindowEntry>> readyForExecution()
	{
		HashMap<Key, List<WindowEntry>> readyEntryGroups = new HashMap<Key, List<WindowEntry>>();
		for(WindowPointer actWinPtr : _fullWindows)
		{
			List<WindowEntry> entryIt = _bucketStore.get(actWinPtr.getVectorIndex()).flush_winSlide();
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

package de.tu_berlin.citlab.storm.bolts;

import java.lang.reflect.Array;

import de.tu_berlin.citlab.storm.spouts.FieldKeys;

public class BucketStore<WindowEntry>
{
	private final SlidingWindow<WindowEntry>[] _bucketStore;
	private final FieldKeys _fieldKeys;
	
	@SuppressWarnings("unchecked")
	public BucketStore(FieldKeys fieldKeys, long windowSize)
	{
		//Generic Workaround for "new SlidingWindow<BucketData>>[FieldKeys.values().length];":
		_bucketStore = (SlidingWindow<WindowEntry>[]) Array.newInstance(SlidingWindow.class, FieldKeys.values().length);
		_fieldKeys = fieldKeys;
		
		this.init_bucketStore(windowSize);
	}


	public boolean sortInBucket(WindowEntry input, int sortID)
	{ 
		boolean added = _bucketStore[sortID].appendEntry(input);
		return added;
	}
	
	public WindowEntry[] prepare_emission()
	{
		for(SlidingWindow actWindow : _bucketStore)
		if(actWindow.check_sizeReached())
		{
			//WindowEntry TODO: go on here!
		}
	}
	
	
	private void init_bucketStore(long windowSize) 
	{
		for(int n = 0 ; n < _bucketStore.length ; n++)
		{
			_bucketStore[n] = new SlidingWindow<WindowEntry>(windowSize);
		}
	}
}

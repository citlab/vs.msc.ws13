package stormCG.bolts;

import java.util.Iterator;

public class EntryGroup<Key extends Comparable<Key>, Entry> 
{
	public Key _key;
	public Iterator<Entry> _iterator;
	
	public EntryGroup(Key key, Iterator<Entry> it)
	{
		_key = key;
		_iterator = it;
	}
}

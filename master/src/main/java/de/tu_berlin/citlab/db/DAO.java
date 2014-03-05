package de.tu_berlin.citlab.db;

import java.util.List;

import backtype.storm.tuple.Tuple;

public interface DAO
{
	void store(List<Tuple> tuples);
	void analyzeTuple(Tuple tuple);
	void init();
	void createDataStructures();
	DBConfig createConfig();
}

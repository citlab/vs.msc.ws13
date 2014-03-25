package de.tu_berlin.citlab.storm.udf;

import backtype.storm.tuple.Fields;

import java.io.Serializable;

public interface UDFOutput extends Serializable{
    public Fields getOutputFields();
}


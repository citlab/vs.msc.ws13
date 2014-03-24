package de.tu_berlin.citlab.storm.udf;

import java.io.Serializable;
import java.util.List;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;

public abstract class IOperator implements Serializable {
	public void execute(List<Tuple> input, OutputCollector collector) throws OperatorException {

    }
    private UDFBolt bolt;
    public void setUDFBolt(UDFBolt bolt){
        this.bolt=bolt;
    }
    public UDFBolt getUDFBolt(){ return this.bolt; }
}

package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.bolts.UDFBolt;
import de.tu_berlin.citlab.storm.exceptions.OperatorException;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class MultipleOperators extends IOperator {

    private Map<String,OperatorProcessingDescription> operators = new HashMap<String, OperatorProcessingDescription>();

    public MultipleOperators( OperatorProcessingDescription... descs ) {

        for( OperatorProcessingDescription desc : descs ) {
            operators.put(desc.getSource(), desc );
        }
    }

    @Override
    public void setUDFBolt(UDFBolt bolt){
        Iterator it =  operators.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            operators.get(pairs.getKey()).getOperator().setUDFBolt(bolt);
        }//while
    }


    @Override
    public void execute(List<Tuple> input, OutputCollector collector) throws OperatorException {

        String source = input.get(0).getSourceComponent();
        if( operators.containsKey( source ) ){

            // execute operator
            operators.get(source).getOperator().execute( input, collector );

        } else {
            System.err.print("ERROR: could not find appropriate operator in MultipleOperators for source "+source );
        }
    }
}

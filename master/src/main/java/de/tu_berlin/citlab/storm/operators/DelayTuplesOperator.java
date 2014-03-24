package de.tu_berlin.citlab.storm.operators;

import backtype.storm.task.OutputCollector;
import backtype.storm.tuple.Tuple;
import de.tu_berlin.citlab.storm.udf.IOperator;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class DelayTuplesOperator extends IOperator {
    class DelayedTuple{
        private Long created=System.currentTimeMillis();
        private Tuple p;
        public DelayedTuple(Tuple p){
            this.p = p;
        }
        public Long getCreatedTime(){ return created; }
        public Tuple getTuple(){ return p; }
    }

    private Queue<DelayedTuple> queue = new LinkedList<DelayedTuple>();
    private int delaySec;

    public DelayTuplesOperator(int sec){
        delaySec = sec;
    }

    @Override
    public void execute(List<Tuple> tuples, OutputCollector collector) {
        for( Tuple p : tuples ){
            queue.add( new DelayedTuple(p));
        }//for

        while( !queue.isEmpty() && (System.currentTimeMillis()-queue.element().getCreatedTime()) >= delaySec*1000 ){
            collector.emit( queue.poll().getTuple().getValues() );
        }

        this.getUDFBolt().log_debug("operator", "delayed tuples: queue-size: "+queue.size() );

    }//execute()
}
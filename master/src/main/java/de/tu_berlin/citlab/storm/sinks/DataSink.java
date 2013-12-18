package de.tu_berlin.citlab.storm.sinks;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import de.tu_berlin.citlab.storm.udf.Context;
import de.tu_berlin.citlab.storm.udf.IOperator;
import de.tu_berlin.citlab.storm.window.DataTuple;

public class DataSink implements IOperator {
    private static int instance = 0;

	 class Worker extends Thread {
		
	    private final Queue<List<DataTuple>> queue;
	    
	    private IOperator worker;
	    
	    public Worker(Queue<List<DataTuple>> queue, IOperator worker) {
	    	this.queue = queue;
	        setName("MyWorker:" + (instance++));
	    }
	     
	    @Override
	    public void run() {
	        while ( true ) {
	            try {
	            	List<DataTuple> tupleBunch = null;
	 
	                synchronized ( queue ) {
	                    while ( queue.isEmpty() ){
	                    	queue.wait();
	                    }
	                    // Get the next work item off of the queue
	                    tupleBunch = queue.poll();
	                }
	                
	                // Process the work item
	                worker.execute(tupleBunch, null );
	            }
	            catch ( InterruptedException ie ) {
	                break;  // Terminate
	            }
	        }
	    }
	}
	
	private static final long serialVersionUID = -6563062550998641926L;

	private Queue<List<DataTuple>> processingQueue = new LinkedList<List<DataTuple>>();
	
	private IOperator op;
	
	public DataSink( IOperator op ){
		this.op = op;
		
		// create one thread, more are possible.
		Worker w = new Worker(processingQueue, op);
		w.start();
		
	}
	
	@Override
	public List<DataTuple> execute(List<DataTuple> tuples, Context context) {

		//processingQueue.add( tuples );
		//processingQueue.notify();
		
		op.execute( tuples, context);
		
		return tuples;
	}

}

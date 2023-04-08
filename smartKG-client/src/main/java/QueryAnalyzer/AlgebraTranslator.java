/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpConditional;
import org.apache.jena.sparql.algebra.op.OpDistinct;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpReduced;
import org.apache.jena.sparql.algebra.op.OpSequence;
import org.apache.jena.sparql.algebra.op.OpSlice;
import org.apache.jena.sparql.algebra.op.OpUnion;

/**
 *
 * @author azzam
 */
public class AlgebraTranslator extends OpVisitorBase {

    /**
     * All prefixes
     
    private final PrefixMapping prefixes;
    */
    /**
     * All operators to execute
     */
    //private Queue<SparkOp> executionQueue = new LinkedList<>();
    private final PrefixMapping prefixes;
    private QueryStructure queryStructure = QueryStructure.getInstance();
    /**
     * Create a new translator
     * 
     * @param prefixes
     *            All prefixes to use
     */
    public AlgebraTranslator(PrefixMapping prefixes) {
	this.prefixes = prefixes;
    }

    /**
     * Get the queue with the {@link SparkOp}s
     * 
     * @param opBGP
     * @return Queue of {@link SparkOp}s
     
    public Queue<SparkOp> getExecutionQueue() {
	return executionQueue;
    }
*/
    @Override
    public void visit(OpBGP opBGP) {
        
       // System.out.println("OpBGP-->"  + opBGP.toString());
        queryStructure.setBGP(opBGP);
	/*SparkBGP bgp = new SparkBGP(opBGP, prefixes);
	
	 * Add it twice. The first run will match the BGP and the second run
	 * will build the result. This is done for the exact time measuring and
	 * could basically done with one object but then without exact time
	 * measuring.
	 
	executionQueue.add(bgp);
	executionQueue.add(bgp);*/
    }

    @Override
    public void visit(OpFilter opFilter) {
	//System.out.println("opFilter-->"  + opFilter.toString());
        queryStructure.setFilter(opFilter);
        /*opBGP
        executionQueue.add(new SparkFilter(opFilter, prefixes));
	addExpressionsToBGP(opFilter.getExprs());*/
    }

    @Override
    public void visit(OpJoin opJoin) {
        
  //  System.out.println("opJoin-->"  + opJoin.toString());
    queryStructure.setJoin(opJoin);

    }

    @Override
    public void visit(OpSequence opSequence) {
        
   //  System.out.println("opSequence-->"  + opSequence.toString());
        
    }

    @Override
    public void visit(OpLeftJoin opLeftJoin) {
	/*executionQueue.add(new SparkLeftJoin(opLeftJoin, prefixes));
	if (opLeftJoin.getExprs() != null) {
	    addExpressionsToBGPLeftJoin(opLeftJoin.getExprs());
	}*/
        
      //  System.out.println("opLeftJoin-->"  + opLeftJoin.toString());
         queryStructure.setLeftJoin(opLeftJoin);
    }

    @Override
    public void visit(OpConditional opConditional) {
        
   //System.out.println("opConditional-->"  + opConditional.toString());

    }

    @Override
    public void visit(OpUnion opUnion) {
	//executionQueue.add(new SparkUnion(opUnion));
        
  // System.out.println("opUnion-->"  + opUnion.toString());
        queryStructure.setUnion(opUnion);
    }

    @Override
    public void visit(OpProject opProject) {
        
	//executionQueue.add(new SparkProjection(opProject));
    // System.out.println("opProject-->"  + opProject.toString());
     queryStructure.setProject(opProject);

    }

    @Override
    public void visit(OpDistinct opDistinct) {
	//executionQueue.add(new SparkDistinct(opDistinct));
        //   System.out.println("opDistinct-->"  + opDistinct.toString());
           
    }

    @Override
    public void visit(OpOrder opOrder) {
	//executionQueue.add(new SparkOrderBy(opOrder, prefixes));
        //   System.out.println("opOrder-->"  + opOrder.toString());
        queryStructure.setOrder(opOrder);
    }

    @Override
    public void visit(OpSlice  opSlice) {
	//executionQueue.add(new SparkSlice(opSlice));
         //  System.out.println("opSlice-->"  + opSlice.toString());

    }

    @Override
    public void visit(OpReduced opReduced) {
   // System.out.println("opReduced-->"  + opReduced.toString());

    } 

    /**
     * Put an expression into an Basic Graph Pattern if it can be executed
     * directly
     * 
     * @param expr
     *            Expressions that should be added
     */
   /* private void addExpressionsToBGP(ExprList expr) {
	Stack<SparkOp> stack = new Stack<SparkOp>();

	while (!executionQueue.isEmpty()) {
	    stack.push(executionQueue.poll());
	}

	int distance = 0;
	Boolean added = false;

	Stack<SparkOp> stack2 = new Stack<SparkOp>();

	while (!stack.isEmpty()) {
	    SparkOp actual = stack.pop();
	    stack2.push(actual);

	    // Filter is directly next to the BGP
	    if (actual instanceof SparkBGP && distance == 1) {
		((SparkBGP) actual).addExpressions(expr);
		added = true;
	    }
	    distance = distance + 1;
	}

	int itr = 0;
	while (!stack2.isEmpty()) {
	    SparkOp actual = stack2.pop();
	    executionQueue.add(actual);
	    if (!added && itr == 0 && actual instanceof SparkBGP) {
		((SparkBGP) actual).addExpressions(expr);
		itr++;
	    }
	}
    }*/

    /**
     * Add an expression to a left join if it can be executed with it
     * 
     * @param expr
     *            Expressions to execute
     
    private void addExpressionsToBGPLeftJoin(ExprList expr) {
	Stack<SparkOp> stack = new Stack<SparkOp>();

	while (!executionQueue.isEmpty()) {
	    stack.push(executionQueue.poll());
	}

	Stack<SparkOp> stack2 = new Stack<SparkOp>();
	int distance = 0;

	while (!stack.isEmpty()) {
	    SparkOp actual = stack.pop();
	    stack2.push(actual);

	    if (actual instanceof SparkBGP && distance == 1) {
		((SparkBGP) actual).addExpressions(expr);
	    }
	    distance = distance + 1;
	}

	while (!stack2.isEmpty()) {
	    SparkOp actual = stack2.pop();
	    executionQueue.add(actual);
	}
    }*/
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import java.util.Iterator;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpVisitor;
import org.apache.jena.sparql.algebra.OpVisitorByType;
import org.apache.jena.sparql.algebra.op.Op0;
import org.apache.jena.sparql.algebra.op.Op1;
import org.apache.jena.sparql.algebra.op.Op2;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpN;

/**
 *
 * @author azzam
 */
public class AlgebraWalker extends OpVisitorByType{

    private final OpVisitor visitor;
    
    public AlgebraWalker(OpVisitor visitor) {
	this.visitor = visitor;
    }

    @Override
    protected void visitN(OpN opn) {
        for (Iterator<Op> iter = opn.iterator(); iter.hasNext();) {
	    Op sub = iter.next();
	    sub.visit(this);
	}
	opn.visit(visitor);
    }

    @Override
    protected void visit2(Op2 op2) {
        if (op2.getLeft() != null)
	    op2.getLeft().visit(this);
	if (op2.getRight() != null)
	    op2.getRight().visit(this);
            op2.visit(visitor); 
    }

    @Override
    protected void visit1(Op1 op1) {
        if (op1.getSubOp() != null)
	    op1.getSubOp().visit(this);
	op1.visit(visitor);    }

    @Override
    protected void visit0(Op0 op0) {
	op0.visit(visitor);
    }

    @Override
    protected void visitFilter(OpFilter of) {
        if (of.getSubOp() != null)
	    of.getSubOp().visit(this);
            of.visit(visitor);  
    }

    @Override
    protected void visitLeftJoin(OpLeftJoin olj) {
        if (olj.getLeft() != null)
	    olj.getLeft().visit(this);
	if (olj.getRight() != null)
	    olj.getRight().visit(this);
            
        olj.visit(visitor);
    }
    
}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatahdtfragment.model;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.rdfhdt.hdt.rdf.parsers.JenaNodeCreator;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

/**
 *
 * @author azzam
 */
public class ExtendedTripleIteratorHdt implements ExtendedIterator<Triple> {

     
    protected Iterator<IteratorTripleString> hdtTotalIterator;
    protected IteratorTripleString tripleIterator;

    public ExtendedTripleIteratorHdt(ArrayList<IteratorTripleString> hdtTotalResults) {
       
      //  System.out.println("htdTotalResults:" + htdTotalResults.size());
         
        this.hdtTotalIterator = hdtTotalResults.iterator();
       //htdTotalIterator.forEachRemaining((t) -> {
        //    System.out.println("subsetIterator:" + t.next());
       // });
        if (hdtTotalIterator.hasNext()) {
          //  System.out.println("hiiiiiiiiiiiiiiiiiii");
            tripleIterator = hdtTotalIterator.next();
            
           // System.out.println("tripleIterator" + tripleIterator);
        }
    }

    static ExtendedIterator<Triple> create(ArrayList<IteratorTripleString> htdTotalResults) {
       // System.out.println("next =>");
        return new ExtendedTripleIteratorHdt(htdTotalResults);

    }

    @Override
    public boolean hasNext() {
        
       // System.out.println("HasNext");
        if (tripleIterator == null) return false;
        boolean hasNext = tripleIterator.hasNext();
        if (!hasNext) {
            if (hdtTotalIterator.hasNext()) {
                tripleIterator = hdtTotalIterator.next();
                return tripleIterator.hasNext();
            } else {
                return false;
            }
        } else {
            return hasNext;
        }

    }

    @Override
    public Triple next() {
       // System.out.println("Next");
        if (tripleIterator == null) return null;
        boolean hasNext = tripleIterator.hasNext();
        if (!hasNext) {
            if (hdtTotalIterator.hasNext()) {
                tripleIterator = hdtTotalIterator.next();
                if (tripleIterator.hasNext()) {

                    TripleString ts = tripleIterator.next();
                    Triple res = createTriple(ts);
                    
                   // System.out.println("Triple =>" + res);
                    return res;
                } else {
                    return null;
                }
            } else {
                return null;
            }
        } else {

            TripleString ts = tripleIterator.next();
            Triple res = createTriple(ts);
            return res;
        }

    }

    private Triple createTriple(TripleString ts) {
        Node subjectNode = createNode(ts.getSubject());
        Node predicateNode = createNode(ts.getPredicate());
        Node objectNode = createNode(ts.getObject()); 
        
        return new Triple(subjectNode, predicateNode, objectNode);
    }
    
     private Node createNode(CharSequence charSeq) {
     
         Node node;
        char firstChar = charSeq.charAt(0);
        switch (firstChar) {
            case '_':
                node = JenaNodeCreator.createAnon(charSeq.toString());
                break;
            case '"':
                node = JenaNodeCreator.createLiteral(charSeq.toString());
                break;
            default:
                node = JenaNodeCreator.createURI(charSeq.toString());
                break;
        }
       return node;
     }


    @Override
    public Triple removeNext() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <X extends Triple> ExtendedIterator<Triple> andThen(Iterator<X> other) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ExtendedIterator<Triple> filterKeep(Predicate<Triple> predicate) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ExtendedIterator<Triple> filterDrop(Predicate<Triple> predicate) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <U> ExtendedIterator<U> mapWith(Function<Triple, U> function) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public List<Triple> toList() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Set<Triple> toSet() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public void close() {
        //System.out.print("No need");
        
    }

   
}

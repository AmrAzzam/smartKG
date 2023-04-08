/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatahdtfragment.model;

import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.io.IOUtils;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.linkeddatafragments.model.LinkedDataFragment;
import org.rdfhdt.hdt.rdf.parsers.JenaNodeCreator;
import org.rdfhdt.hdt.triples.IteratorTripleString;


/**
 *
 * @author azzam
 */
public class ExtendedTripleIteratorNodeLDF implements ExtendedIterator<Triple> {

    protected InputStream is;
    protected InputStreamReader isr;
    protected BufferedReader br;
    String line;
    // protected Iterator<Triple> nodeIterator;
    /* protected Triple triple;
    
    static Iterator<String> streamLDF;
    
    private Stream<String> stream;*/

    ProcessBuilder builder;
    Stopwatch stopwatch;

    public ExtendedTripleIteratorNodeLDF(InputStream is,Stopwatch stopwatch) {

        this.is = is;
        this.isr = new InputStreamReader(is);
        this.br = new BufferedReader(isr);
        this.stopwatch = stopwatch;
       //try {
            //System.out.println("available =>" + is.available());

            //this String cmd = "/home/azzam/Desktop/test.sh";
            //streamLDF = br.lines().collect(Collectors.toList()).iterator();
            //this.stream = is;
            // streamLDF = stream.iterator();
            //builder.start();
      //  } catch (IOException ex) {
      //      Logger.getLogger(ExtendedTripleIteratorNodeLDF.class.getName()).log(Level.SEVERE, null, ex);
      //  }

    }

    static ExtendedIterator<Triple> create(InputStream is,Stopwatch stopwatch) {
        return new ExtendedTripleIteratorNodeLDF(is, stopwatch);
    }

    @Override
    public boolean hasNext() {
        try {
            
            System.out.println("StarthasNext()" + stopwatch.elapsed(TimeUnit.MILLISECONDS));

            /*  if (streamLDF.hasNext()) {
            line = streamLDF.next();
            return true;}*/
            //IOUtils.
            line = br.readLine();
            System.out.println("EndhasNext()" + stopwatch.elapsed(TimeUnit.MILLISECONDS));

            if (line != null) {
                // System.out.println("Line => " + line);
                return true;
            } else {
                //System.out.println("Line false => " + line);
                
                return false;
            }
        } catch (IOException ex) {
            Logger.getLogger(ExtendedTripleIteratorNodeLDF.class.getName()).log(Level.SEVERE, null, ex);
        }
        return false;
    }

    @Override
    public Triple next() {

       // boolean hasNext = this.hasNext();
       System.out.println("StartNext()" + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        if (line != null) {
            String[] nodes = line.split(" ");
            Node subjectNode = createNode(nodes[0]);
            Node predicateNode = createNode(nodes[1]);
            Node objectNode = createNode(nodes[2]);
            System.out.println("StartNext()" + stopwatch.elapsed(TimeUnit.MILLISECONDS));

            return new Triple(subjectNode, predicateNode, objectNode);
        } else {
            //String line = streamLDF.next();
            //System.out.print("Triple next():" + line);
            
            return null;
            

        }

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
    public ExtendedIterator<Triple> filterKeep(Predicate<Triple> f) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public ExtendedIterator<Triple> filterDrop(Predicate<Triple> f) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public <U> ExtendedIterator<U> mapWith(Function<Triple, U> map1) {
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
        //System.out.println("Close");
    }

}

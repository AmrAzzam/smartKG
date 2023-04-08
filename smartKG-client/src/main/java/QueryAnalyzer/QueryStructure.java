/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.reasoner.TriplePattern;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpFilter;
import org.apache.jena.sparql.algebra.op.OpJoin;
import org.apache.jena.sparql.algebra.op.OpLeftJoin;
import org.apache.jena.sparql.algebra.op.OpOrder;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpUnion;
import org.apache.jena.sparql.core.Var;
import org.linkeddatahdtfragment.model.TripleWrapper;

/**
 *
 * @author azzam
 */
public class QueryStructure {

    private static final QueryStructure INSTANCE = new QueryStructure();

    // prefix declarations
    private PrefixMapping prefixes;
    // dataset definition

    // query pattern
    private List<Triple> queryBGP;
    private List<OpJoin> queryJoins;
    private List<OpLeftJoin> queryLeftJoins;
    private List<OpUnion> queryUnions;
    //query modifiers
    private List<OpOrder> queryOrders;
    private List<OpFilter> queryFilters;
    private List<Var> queryProjectVars;
    // Star Star ID , List of TP in each Star
    private Map<String, List<Triple>> queryStars;
    private HashMap< TripleWrapper, String> TripleByStar;

    private QueryStructure() {
        queryBGP = new ArrayList<>();
        queryJoins = new ArrayList<>();
        queryLeftJoins = new ArrayList<>();
        queryUnions = new ArrayList<>();
        //query modifiers
        queryOrders = new ArrayList<>();
        queryFilters = new ArrayList<>();
        queryProjectVars = new ArrayList<>();
        queryStars = new  HashMap<>();
        TripleByStar = new HashMap<>();
                

    }

    public static QueryStructure getInstance() {
        return INSTANCE;
    }

    public void parseQuery(Query query) {

        this.prefixes = query.getPrefixMapping();
        Op opRoot = Algebra.compile(query);
        AlgebraTranslator trans = new AlgebraTranslator(prefixes);
        AlgebraWalker walker = new AlgebraWalker(trans);
        opRoot.visit(walker);
    }

    void setProject(OpProject opProject) {
        this.queryProjectVars = opProject.getVars();
    }

    void setFilter(OpFilter opFilter) {
        queryFilters.add(opFilter);
    }

    void setOrder(OpOrder opOrder) {
        queryOrders.add(opOrder);
    }

    void setUnion(OpUnion opUnion) {
        queryUnions.add(opUnion);
    }

    public void setBGP(OpBGP op) {
        queryBGP = op.getPattern().getList();
     //   System.out.println("==============SetBGP================");
      // System.out.println(queryBGP);
    /*   queryStars = queryBGP.stream().collect(
                Collectors.groupingBy(Triple::getSubject,
                         Collectors.toList())  HashMap <Node,Integer> localId = new HashMap<>(););
     */
        
         int count = 0;
         HashMap <Node,Integer> localId = new HashMap<>();
          for (Triple tp : queryBGP)
          {
            if (InfrequentPredicatesConfig.getInstance().checkInfrequentPredicates(tp.getPredicate().toString()))
            {
                System.out.println("InfrequentPredicatesConfig");
                continue;
            }
            
                Node subject = tp.getSubject();
              Integer localFamily ;
              if ((localFamily = localId.get(subject)) == null)
               {     
                    localFamily = count++;  
                    localId.put(subject, localFamily);
               }     
                String localKey = op.hashCode() + "-"+ localFamily;
                queryStars.computeIfAbsent(localKey, k -> new ArrayList<>()).add(tp);
                TripleWrapper tripleWrapper = new TripleWrapper(tp.getSubject(),tp.getPredicate(),tp.getObject());
                TripleByStar.put(tripleWrapper, localKey);
                
          }
          
         // System.out.println("queryStars");
         // queryStars.entrySet().forEach(s -> System.out.println(s));
    }
  

    void setJoin(OpJoin opJoin) {
        queryJoins.add(opJoin);
    }

    void setLeftJoin(OpLeftJoin opLeftJoin) {
        queryLeftJoins.add(opLeftJoin);
    }

    public PrefixMapping getPrefixes() {
        return prefixes;
    }

    public List<Triple> getQueryBGP() {
        return queryBGP;
    }

    public List<OpJoin> getQueryJoins() {
        return queryJoins;
    }

    public List<OpLeftJoin> getQueryLeftJoins() {
        return queryLeftJoins;
    }

    public List<OpUnion> getQueryUnions() {
        return queryUnions;
    }

    public List<OpOrder> StringgetQueryOrders() {
        return queryOrders;
    }

    public List<OpFilter> getQueryFilters() {
        return queryFilters;
    }

    public List<Var> getQueryProjectVars() {
        return queryProjectVars;
    }
    
    public Map<String, List<Triple>> getQueryStars() {
        return queryStars;
    }
    
      public HashMap<TripleWrapper, String> getTripleByStar() {
        return TripleByStar;
    }
}

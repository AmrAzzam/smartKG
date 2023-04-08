/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import com.google.common.base.Stopwatch;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.reasoner.TriplePattern;
import org.linkeddatahdtfragment.model.TripleWrapper;

/**
 *
 * @author azzam
 */
public class QueryAnalyzer {

    private final Query query;
    private final FamiliesConfig familiesconfig;
    private final HashMap<Node, List> familiesHashedByPredicate;
    private final Map<String, List<Triple>> queryStars;
    private final List<Triple> queryTriples;
    private final HashMap<TripleWrapper, String> TripleByStar;

    public QueryAnalyzer(Query query) {
        this.query = query;

        // Query Parsing
        QueryStructure queryStructure = QueryStructure.getInstance();
        queryStructure.parseQuery(query);

        // Initialize The List of The query triple patters grouped in Stars.
        queryStars = queryStructure.getQueryStars();
        //   System.out.println("QueryStars===>");
        queryTriples = queryStructure.getQueryBGP();
        TripleByStar = queryStructure.getTripleByStar();

        // Inititalize The Partitions Configurations from the Server
        familiesconfig = FamiliesConfig.getInstance();
        familiesHashedByPredicate = familiesconfig.getFamiliesByPredicate();

    }

    /*
      Integer (StarID), List<
     */
    public HashMap<String, List<Integer>> getQueryStarsFamilies() {

        HashMap<String, List<Integer>> queryStarsFaimilies = new HashMap<>();

        // We will exclude any query star that consists of 1 predicate as we will use LDF to solve it.
        //  System.out.println("getQueryStarsFamilies: " + queryStars);
        //Stopwatch stopwatch = Stopwatch.createStarted();
       // System.out.println("Time elapsed for Before: " + stopwatch.elapsed(TimeUnit.SECONDS));
        queryStars.entrySet().stream().
                filter(entry -> entry.getValue().size() > 1).
                forEach(entry -> {
                    // System.out.println("===>" + entry.getValue().get(0).getPredicate());
                    List<Triple> sortedTripleList = entry.getValue();

                    // System.out.println("sortedTripleList =>" + sortedTripleList);
                    sortedTripleList.sort((o1, o2) -> {
                        //   System.out.println(o1.getPredicate() + "<= O1 =>" + familiesHashedByPredicate.get(o1.getMatchPredicate()).size());
                        //  System.out.println(o2.getPredicate() + "<= O2 =>" + familiesHashedByPredicate.get(o2.getMatchPredicate()).size());
                        if (familiesHashedByPredicate.get(o1.getPredicate()).size() < familiesHashedByPredicate.get(o2.getPredicate()).size()) {
                            return -1;
                        }
                        return 0;
                    });

                    //System.out.println("sortedTripleList =>" + sortedTripleList);
                    List<Integer> starQueryFamilies = familiesHashedByPredicate.
                            get(sortedTripleList.get(0).getPredicate());

                    if (starQueryFamilies != null) {

                        for (Triple tp : entry.getValue()) {
                            //  System.out.println("Time elapsed for " + tp + " Before ==> " + stopwatch.elapsed(TimeUnit.SECONDS));
                            //System.out.println("starQueryFamilies" + tp);
                            // System.out.println("Before starQueryFamilies=>"+ tp + "  " + starQueryFamilies.size() );
                            // System.out.println("Size: " + familiesHashedByPredicate.get(tp.getPredicate()).size());

                           // starQueryFamilies = starQueryFamilies.stream().
                             //       filter(familiesHashedByPredicate.get(tp.getPredicate())::contains)
                               //    .collect(Collectors.toList());
                              //starQueryFamilies.retainAll(familiesHashedByPredicate.get(tp.getPredicate()));
                           starQueryFamilies = intersection(starQueryFamilies, familiesHashedByPredicate.get(tp.getPredicate()));
                        }
                        //    System.out.println("Time elapsed for " + tp + " After ==> " + stopwatch.elapsed(TimeUnit.SECONDS));
                        //  System.out.println("After starQueryFamilies=>"+ tp + "  " + starQueryFamilies.size() );
                        //System.out.println("starQueryFamilies" + tp);

                       //System.out.println("Time elapsed for Middle: " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

                        List<Integer> starQueryGroupedFamilies = starQueryFamilies.stream().filter(familyId
                                -> FamiliesConfig.getInstance().getFamilyByID(familyId).isGrouped()).collect(Collectors.toList());

                        // starQueryGroupedFamilies.forEach((star) -> System.out.println(star));
                        if (starQueryGroupedFamilies.isEmpty()) {
                         //   System.out.println("starQueryGroupedFamilies == null");
                            queryStarsFaimilies.put(entry.getKey(), starQueryFamilies);
                        } else if (starQueryGroupedFamilies.size() == 1) {
                         //   System.out.println("starQueryGroupedFamilies.size() == 1");
                            queryStarsFaimilies.put(entry.getKey(), starQueryGroupedFamilies);
                        } else if (starQueryGroupedFamilies.size() > 1) {
                          //  System.out.println("Time elapsed for End: " + stopwatch.elapsed(TimeUnit.SECONDS));

                          //  System.out.println("starQueryGroupedFamilies > 1" + starQueryGroupedFamilies.size());
                            //  starQueryGroupedFamilies.stream().forEach(x -> System.out.println(x));
                            Optional<Integer> PartitionId = starQueryGroupedFamilies.stream().min((f1, f2) -> Integer.compare(
                                    FamiliesConfig.getInstance().getFamilyByID(f1).getPredicateSet().size(),
                                    FamiliesConfig.getInstance().getFamilyByID(f2).getPredicateSet().size()));
                            //      System.out.println("starQueryGroupedFamilies > 1" + PartitionId);
                            Family familySolution = FamiliesConfig.getInstance().getFamilyByID(PartitionId.get());

                            //  System.out.println("Family Solution: " + familySolution.getIndex());
                            List<Integer> sourceSet = familySolution.getSourceSet();
                            //  System.out.println("sourceSet: " + familySolution.getSourceSet());
                            if (sourceSet != null) {
                                //     System.out.println("It is a source Set :D");
                                if (familySolution.isOriginalFamily()) {
                                    sourceSet.add(familySolution.getIndex());
                                }
                                queryStarsFaimilies.put(entry.getKey(), sourceSet);
                            } else {
                                //       System.out.println("It is a source Set :)");
                                queryStarsFaimilies.put(entry.getKey(), new ArrayList<>(Arrays.asList(PartitionId.get())));
                            }
                        } else {
                            // System.out.println("1 predicate star");
                            List<Integer> starQueryDetailedFamilies = starQueryFamilies.stream().filter(
                                    familyId -> !(FamiliesConfig.getInstance().getFamilyByID(familyId)).isGrouped()).collect(Collectors.toList());
                            queryStarsFaimilies.put(entry.getKey(), starQueryDetailedFamilies);

                        }

                    } else {
                        // System.out.println("1 predicate star");
                        // This is to initialize the 1 predicate star with empty family so it could be executed using LDF
                        queryStarsFaimilies.put(entry.getKey(), new ArrayList<>());
                    }
                });
        // System.out.println("starQueryFamilies: " + queryStarsFaimilies);
        return queryStarsFaimilies;
    }

    public HashMap<TripleWrapper, String> getTripleByStar() {
        return TripleByStar;
    }

    private Integer[] intersection(int firstArray[], int secondArary[]) {
        int i = 0;
        int j = 0;
        ArrayList<Integer> list = new ArrayList<Integer>();
        while (i < firstArray.length && j < secondArary.length) {
            if (firstArray[i] < secondArary[j]) {
                i++;// Increase I move to next element
            } else if (secondArary[j] < firstArray[i]) {
                j++;// Increase J move to next element
            } else {
                list.add(secondArary[j++]);
                i++;// If same increase I & J both
            }
        }
        return list.toArray(new Integer[0]);
    }

    private ArrayList<Integer> intersection(List<Integer> starQueryFamilies, List<Integer> tripleFamilyList) {
        int i = 0;
        int j = 0;
        ArrayList<Integer> intersectedList = new ArrayList<>();
        while (i < starQueryFamilies.size() && j < tripleFamilyList.size()) {
            if (starQueryFamilies.get(i) < tripleFamilyList.get(j)) {
                i++;// Increase I move to next element
            } else if (tripleFamilyList.get(j) < starQueryFamilies.get(i)) {
                j++;// Increase J move to next element
            } else {
                intersectedList.add(tripleFamilyList.get(j));
                i++;// If same increase I & J both
            }
        }
        return intersectedList;

    }

}

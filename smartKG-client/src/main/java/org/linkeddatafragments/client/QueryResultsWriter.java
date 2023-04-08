package org.linkeddatafragments.client;

import Evaluation.Evaluation;
import QueryAnalyzer.Downloader.FamiliesHdtCache;
import QueryAnalyzer.QueryAnalyzer;
import com.google.common.base.Stopwatch;
import java.io.BufferedReader;
import java.io.BufferedWriter;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.PrefixMapping;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;
import org.linkeddatafragments.utils.Config;
import org.linkeddatahdtfragment.model.LinkedDataHdtFragmentGraph;
import org.linkeddatahdtfragment.model.TripleWrapper;

/**
 * A QueryResultsWriter executes a query on a graph and writes its results to a
 * stream.
 *
 * @author Laurens De Vocht
 * @author Ruben Verborgh
 */
public class QueryResultsWriter {

    private final Model model;
    private final Query query;

    /**
     * Creates a new QueryResultsWriter.
     *
     * @param model The model to query.
     * @param query The query to execute.
     */
    public QueryResultsWriter(final Model model, final Query query) {
        // System.out.println("start of QueryResultsWriter");
        this.model = model;
        this.query = query;

        // System.out.println("end of QueryResultsWriter");
    }

    /**
     * Write the results of the query to the stream.
     *
     * @param outputStream The output stream.
     */
    public void writeResults(final PrintStream outputStream) {

        // System.out.println("Before QueryExecution results :)");
        final QueryExecution executor = QueryExecutionFactory.create(query, model);
        // System.out.println("After QueryExecution results :)");
        switch (query.getQueryType()) {
            case Query.QueryTypeSelect:
                //       System.out.println("Before executor :)");

                final ResultSet rs = executor.execSelect();

                Evaluation.measureRecieveFirstAnswerTimePerQuery();
                //int count = 0;
                Stopwatch timeOut = Stopwatch.createStarted();
                int noOfHeavyQueries = 0;
                //executor.setTimeout(Config.getInstance().getTimeInMin(), TimeUnit.MINUTES);
                while (rs.hasNext()) {

                    if (timeOut.elapsed(TimeUnit.MINUTES) >= Config.getInstance().getTimeInMin()) {
                        noOfHeavyQueries++;
                        Evaluation.queryTimeOutPrintCsv();
                        System.out.println("Query =>" + timeOut.elapsed(TimeUnit.MINUTES));
                        break;
                    }

                    Evaluation.resultCountPerQuery++;
                    // System.out.println("Before one query solution :)" );
                    System.out.println(rs.next().toString());
                    // outputStream.println(rs.next().toString());
                    //   System.out.println("After one query solution :)" + rs.getRowNumber());
                    //System.out.println("After one query solution :)" + rs.getRowNumber()); 
                    //  System.out.println("count" + count);
                }
                FamiliesHdtCache.getInstance().releaseCache();
                executor.close();

                System.out.println("NoOfHeavyQueries" + noOfHeavyQueries);
                timeOut.stop();

                break;
            case Query.QueryTypeAsk:
                outputStream.println(executor.execAsk());
                break;
            case Query.QueryTypeConstruct:
            case Query.QueryTypeDescribe:
                final Iterator<Triple> triples = executor.execConstructTriples();
                while (triples.hasNext()) {
                    outputStream.println(triples.next());
                }
                break;
            default:
                throw new Error("Unsupported query type");
        }
        //  System.out.println("End of the wrting");
    }

    /**
     * Starts a standalone version of the results writer.
     *
     * @param args The command-line arguments.
     */
    public static void main(final String[] args) {
        BufferedWriter writer = null;
        // Verify arguments

        //  if (args.length < 1 || args.length > 2 || args[0].matches("/^--?h(elp)?$/")) {
        //l error("usage: java -jar ldf-client.jar [config.json] query");
        //}
        final boolean hasConfig = args.length >= 2;
        Config config = new Config(args[0]);
        System.out.println("Path: " + Config.getInstance().getQueriespath());
        Evaluation ev = new Evaluation();
        Evaluation.startWorkloadTimer();
        int i = 0;
        final LinkedDataFragmentGraph graph = new LinkedDataFragmentGraph(Config.getInstance().getDatasource());
        final LinkedDataHdtFragmentGraph graph1 = new LinkedDataHdtFragmentGraph(Config.getInstance().getDatasource());
         
         
        try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(Paths.get(Config.getInstance().getQueriespath()))) {
            for (Path queryFilePath : directoryStream) {
                i++;
                System.out.println("================" + queryFilePath.getFileName().toString() + "================");

                Evaluation.setQueryID(i);
                Evaluation.setQueryName(queryFilePath.getFileName().toString());
                Evaluation.estimatedNumberOfDownloadedPartitionsPerQuery = 0;
                Evaluation.actualNumberOfDownloadedPartitionsPerQuery = 0;
                Evaluation.estimatedTotalSizeOfDownloadedPartitionsPerQuery = 0;
                Evaluation.actualTotalSizeOfDownloadedPartitionsPerQuery = 0;
                Evaluation.resultCountPerQuery = 0;
                //final String queryFilePath = hasConfig ? args[1] : args[0];
                String queryText;

                queryText = new String(Files.readAllBytes(queryFilePath), StandardCharsets.UTF_8);

                //    TriplePattern tp1 = new TriplePattern("?v0", "<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>", "?v2");
                // Query prefixes must be applied before the query is parsed.
                Query query = QueryFactory.create();

                PrefixMapping pm = PrefixMapping.Factory.create();
                pm.setNsPrefixes(Config.getInstance().getPrefixes());
                query.setPrefixMapping(pm);
                QueryFactory.parse(query, queryText, null, Syntax.syntaxSPARQL);

                boolean linkedData = false;
                if (linkedData) {

                    Evaluation.startQueryTimer();

                    // new QueryResultsWriter(ModelFactory.createModelForGraph(graph), query).writeResults(System.out);
                    ExperimentStarter es = new ExperimentStarter(ModelFactory.createModelForGraph(graph), query);

                    // ModelFactory.createModelForGraph(graph, query);System.out
                    es.writeResults(System.out);
                    Evaluation.stopQueryTimer();// optional
                    Evaluation.measureTotalExecTimePerQuery();
                    //System.out.println("Time elapsed for Results is " + stopwatch.elapsed(TimeUnit.SECONDS));
                    System.out.println(Evaluation.queryEvaluationPrint());
                    Evaluation.queryEvaluationToCsvFile();
                } else {
                    ///  System.out.println("Config.datasource: " + Config.getInstance().getDatasource());
                    //System.out.println("Hiii");

                    Evaluation.startQueryTimer();
                    final QueryAnalyzer queryAnalyzer = new QueryAnalyzer(query);
                    // System.out.println("Time elapsed for MetaData Loading " + stopwatch.elapsed(TimeUnit.SECONDS));
                    // Stopwatch stopwatch = Stopwatch.createStarted();

                    HashMap<TripleWrapper, String> queryTripleByStar = queryAnalyzer.getTripleByStar();

                    //System.out.println("Time elapsed for Analyzing queryTripleByStar: " + stopwatch.elapsed(TimeUnit.SECONDS));
                    HashMap<String, List<Integer>> queryStarsFamilies = queryAnalyzer.getQueryStarsFamilies();

                    //queryStarsFamilies.entrySet().forEach((star) -> System.out.println(star));
                    //System.out.println("Time elapsed for Analyzing queryStarsFamilies: " + stopwatch.elapsed(TimeUnit.SECONDS));
                    Evaluation.measureQueryAnlysisTimePerQuery();
                    // PartitionsDownloader httpDownloader = new HTTPDownloader();
                    // httpDownloader.DownloadPartitionsByStars(queryStarsFamilies);
                    //ftpDownloader.DownloadPartitionsByStars(queryStarsFamilies);

                    //ftpDownloader.closeConnection();
                    Evaluation.measureTotalDownloadTimeOfPartitionsPerQuery();
                    //System.out.println("Time elapsed for Download is " + stopwatch.elapsed(TimeUnit.SECONDS));
                    graph1.setQueryTripleByStar(queryTripleByStar);
                    graph1.setQueryStarsFamilies(queryStarsFamilies);
                    
                    ExperimentStarter es = new ExperimentStarter(ModelFactory.createModelForGraph(graph1), query);
                    // ModelFactory.createModelForGraph(graph, query);System.out
                    es.writeResults(System.out);
                    graph1.closeIterators();
                    //new QueryResultsWriter(ModelFactory.createModelForGraph(graph), query).writeResults(System.out);
                    Evaluation.stopQueryTimer();// optional

                    Evaluation.measureTotalExecTimePerQuery();
                    //System.out.println("Time elapsed for Results is " + stopwatch.elapsed(TimeUnit.SECONDS));
              //      System.out.println(Evaluation.queryEvaluationPrint());
                    Evaluation.queryEvaluationToCsvFile();
                }

            }

            Evaluation.measureTotalDownloadTimeOfPartitionsPerQuery();
            Evaluation.measureTotalExecTimePerWorkload();
            //System.out.println(Evaluation.workloadEvaluationPrint());
            Evaluation.workLoadEvaluationToCsvFile();
        } catch (IOException e) {
            error("Query file could not be read.");
            return;
        }
        Evaluation.stopWorkloadTimer();
        Evaluation.measureTotalExecTimePerWorkload();
        System.exit(0);
        try {
            writer.close();
        } catch (IOException ex) {
            Logger.getLogger(QueryResultsWriter.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    /**
     * Writes the error message and exits the application.
     *
     * @param message The error message.
     */
    private static void error(String message) {
        System.out.print("==============");
        System.err.println(message);
        System.exit(1);
    }
}

/* for (Map.Entry<Node, List<Integer>> entry : querySolutionsPartitions.entrySet()) {
                    Node key = entry.getKey();
                    List<Integer> familiesIds = entry.getValue();

                    for (Integer familyId : familiesIds) {
                       
                        HDT hdt = HDTManager.loadIndexedHDT("path/to/file.hdt", null);
                        HDTGraph graph = new HDTGraph(hdt);
                        Model model = ModelFactory.createModelForGraph(graph);
                        Query hdtQuery = QueryFactory.create("");
                        new QueryResultsWriter(ModelFactory.createModelForGraph(graph), hdtQuery).writeResults(System.out);
                        model.close();
                    }
                }*/

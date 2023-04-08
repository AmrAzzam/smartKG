/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import com.google.gson.Gson;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.linkeddatafragments.utils.Config;

/**
 *
 * @author azzam
 */
public class FamiliesConfig {

    private static FamiliesConfig fc;
    private int numFamilies;
    private List<Family> families;
  

    private FamiliesConfig() {
    }

    public static FamiliesConfig getInstance() {

     
        if (fc == null) {
              Gson gson = new Gson();
            //Load from local
          
           Path metaDatapath = Paths.get(Config.getInstance().getMetadatapath());
         
            File metaData = new File(metaDatapath.toString());
               
            if (metaData.exists())
            {
               // System.out.println("metaData");
                  try {  
                      fc = gson.fromJson( new FileReader(metaData) , FamiliesConfig.class);
                  } catch (FileNotFoundException ex) {
                      Logger.getLogger(FamiliesConfig.class.getName()).log(Level.SEVERE, null, ex);
                  }
            }
            else {
                
                //System.out.println("ServerInteractionHandler");
                    //read from Server
                ServerInteractionHandler sih = new ServerInteractionHandler();
               
                String sb = sih.getServerFamiliesMetaData();
               // System.out.println("SB ==>" + sb.length());
                //Use try-with-resource to get auto-closeable writer instance
               
                fc = gson.fromJson(sb, FamiliesConfig.class);
            }
        }

        return fc;
    }

    public int getNumFamilies() {
        return numFamilies;
    }

    public List<Family> getFamilies() {
        return families;
    }

    public Family getFamilyByID(int ID) {
        return families.get(ID - 1);
    }

    HashMap<Node, List> getFamiliesByPredicate() {

        HashMap<Node, List> familiesHashedByPredicate = new HashMap<>();
        families.forEach(family -> {
            family.getPredicateSet().forEach(predicate -> {

                familiesHashedByPredicate.computeIfAbsent(NodeFactory.createURI(predicate), k -> new ArrayList())
                        .add(family.getIndex());
            });
        });

        return familiesHashedByPredicate;
    }

}

/*

 for(int i = 0; i < families.size();i++)
        {
            List<String> predicates = families.get(i).getPredicateSet();
            
            for (int j = 0; j < predicates.size(); j++)
             {
               predicatesWithALlfamiliesIds.computeIfAbsent(predicates.get(j), k -> new ArrayList()).add(families.get(i).getIndex());
             }
        }
 */

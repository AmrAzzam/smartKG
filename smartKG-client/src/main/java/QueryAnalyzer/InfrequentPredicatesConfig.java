/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer;

import com.google.gson.Gson;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.linkeddatafragments.utils.Config;

/**
 *
 * @author azzam
 */
public class InfrequentPredicatesConfig {
    
    
    private static InfrequentPredicatesConfig ifpc;
    private List<String> infrequentPredicates;

    private InfrequentPredicatesConfig() {
    }

    public static InfrequentPredicatesConfig getInstance() {

     
        if (ifpc == null) {
              Gson gson = new Gson();
            //Load from local
           // File metadata = new File("/home/azzam/Desktop/TPF/ClientAndServer-Java/Client.Java/statsPart_watdiv.json");
            File metadata = new File(Config.getInstance().getMetadatapath());
            if (metadata.exists())
            {
     
                  try {
                      
                     
                      ifpc = gson.fromJson( new FileReader(metadata) , InfrequentPredicatesConfig.class);
                      
                  } catch (FileNotFoundException ex) {
                      Logger.getLogger(FamiliesConfig.class.getName()).log(Level.SEVERE, null, ex);
                  }
            }
            else {
                    //read from Server
               ServerInteractionHandler sih = new ServerInteractionHandler();
               
                String sb = sih.getServerFamiliesMetaData();
                ifpc = gson.fromJson(sb, InfrequentPredicatesConfig.class);
            }
        }

        return ifpc;
    }


    public List<String> getInfrequentPredicates() {
        return infrequentPredicates;
    }

    public boolean checkInfrequentPredicates(String checkPredicate ) {
        return infrequentPredicates.contains(checkPredicate);
    }

   

    
}

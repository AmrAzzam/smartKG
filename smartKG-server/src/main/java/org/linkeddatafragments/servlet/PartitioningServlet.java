/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.linkeddatafragments.servlet;

import com.google.gson.JsonObject;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.FileUtils;
import org.apache.jena.riot.Lang;
import org.linkeddatafragments.config.ConfigReader;
import org.linkeddatafragments.datasource.DataSourceFactory;
import org.linkeddatafragments.datasource.DataSourceTypesRegistry;
import org.linkeddatafragments.datasource.IDataSource;
import org.linkeddatafragments.datasource.IDataSourceType;
import org.linkeddatafragments.datasource.index.IndexDataSource;
import org.linkeddatafragments.exceptions.DataSourceNotFoundException;
import org.linkeddatafragments.fragments.FragmentRequestParserBase;
import static org.linkeddatafragments.servlet.LinkedDataFragmentServlet.CFGFILE;
import org.linkeddatafragments.util.MIMEParse;

/**
 *
 * @author azzam
 */
public class PartitioningServlet extends HttpServlet {

    // <editor-fold defaultstate="collapsed" desc="HttpServlet methods. Click on the + sign on the left to edit the code.">
    /**
     * Handles the HTTP <code>GET</code> method.
     *
     * @param request servlet request
     * @param response servlet response
     * @throws ServletException if a servlet-specific error occurs
     * @throws IOException if an I/O error occurs
     */
    String fileSource = "";
    private ConfigReader config;
    private final HashMap<String, IDataSource> dataSources = new HashMap<>();
    private final Collection<String> mimeTypes = new ArrayList<>();

    private File getConfigFile(ServletConfig config) throws IOException {
        System.out.println("Partitions getConfigFile");
        String path = config.getServletContext().getRealPath("/");
        
        System.out.println("Partitions Path => " + path);
        if (path == null) {
            // this can happen when running standalone
            path = System.getProperty("user.dir");
        }
        File cfg = new File("/home/amr/TPF/ServerFIlteredGroups/Server.Java/config-example.json");
        // /home/amr/TPF/Server.Java/config-example.json
        if (config.getInitParameter(CFGFILE) != null) {
            cfg = new File(config.getInitParameter(CFGFILE));
        }
        if (!cfg.exists()) {
            throw new IOException("Configuration file " + cfg + " not found.");
        }
        if (!cfg.isFile()) {
            throw new IOException("Configuration file " + cfg + " is not a file.");
        }
        return cfg;
    }

    /**
     *
     * @param servletConfig
     * @throws ServletException
     */
    @Override
    public void init(ServletConfig servletConfig) throws ServletException {
        try {
            System.out.println("Partitions loading Configuration File");
            // load the configuration
            File configFile = getConfigFile(servletConfig);
            System.out.println("configFile:" + configFile.getName());
            config = new ConfigReader(new FileReader(configFile));
            
             System.out.println("Partitions ConfigReader =>"  + config.getMetadatapath());
            // register data source types
         //   for (Map.Entry<String, IDataSourceType> typeEntry : config.getDataSourceTypes().entrySet()) {
           //     DataSourceTypesRegistry.register(typeEntry.getKey(),
             //           typeEntry.getValue());
            //}

            // register data sources
            //for (Map.Entry<String, JsonObject> dataSource : config.getDataSources().entrySet()) {
             //   dataSources.put(dataSource.getKey(), DataSourceFactory.create(dataSource.getValue()));
           // }

            // register content types
            MIMEParse.register("text/html");
            MIMEParse.register(Lang.RDFXML.getHeaderString());
            MIMEParse.register(Lang.NTRIPLES.getHeaderString());
            MIMEParse.register(Lang.JSONLD.getHeaderString());
            MIMEParse.register(Lang.TTL.getHeaderString());
        } catch (Exception e) {
            throw new ServletException(e);
        }
    }

    /**
     *metadatapath
     */
    @Override
    public void destroy() {
        for (IDataSource dataSource : dataSources.values()) {
            try {
                dataSource.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    /**
     * Get the datasource
     *
     * @param request
     * @return
     * @throws IOException
     */
    private IDataSource getDataSource(HttpServletRequest request) throws DataSourceNotFoundException {
        String contextPath = request.getContextPath();
        String requestURI = request.getRequestURI();

      //  System.out.println("contextPath =======>" + contextPath);
      //  System.out.println("requestURI =======>" + requestURI);
        String path = contextPath == null
                ? requestURI
                : requestURI.substring(contextPath.length());

        if (path.equals("/") || path.isEmpty()) {
            final String baseURL = FragmentRequestParserBase.extractBaseURL(request, config);
          //  System.out.println("baseURL =======>" + baseURL);
            return new IndexDataSource(baseURL, dataSources);
        }

        String dataSourceName = path.substring(1);
       // System.out.println("dataSourceName =======> " + dataSourceName);
        IDataSource dataSource = dataSources.get(dataSourceName);
        if (dataSource == null) {
            throw new DataSourceNotFoundException(dataSourceName);
        }
        return dataSource;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
            throws ServletException, IOException {

        String uri = request.getRequestURI();
        //System.out.println("URI => "  + uri);

        String[] parsedUri = uri.split("/");
        //System.out.println("parsedUri => "  + parsedUri.length);
        if (parsedUri.length <= 3) {
            //  System.out.println("parsedUri[0]" + parsedUri[0]);
            //  System.out.println("parsedUri[1]" + parsedUri[1]);
            //  System.out.println("parsedUri[2]" + parsedUri[2]);
            // sftp://amr@quantum.ai.wu.ac.at/home/amr/TPF/Server.Java/WebContent/WEB-INF/watdiv/statsPart_watdiv.json
            // fileSource = "/home/amr/TPF/ServerFIlteredGroups/Server.Java/WebContent/WEB-INF/"+ parsedUri[2] + "/statsPart_watdiv.json";  
            System.out.println("metadatapath ==>" + config.getMetadatapath() );
            fileSource = config.getMetadatapath();
            //sftp://amr@quantum.ai.wu.ac.at/home/amr/TPF/ServerFIlteredGroups/Server.Java/WebContent/WEB-INF/watdiv/statsPart_watdiv.json 
            //   /home/amr/TPF/Server.Java/WebContent/WEB-INF/statsPart_watdiv.json
        } else {
            // System.out.println("The hdt files");
            // System.out.println("parsedUri[0]" + parsedUri[0]);
            // System.out.println("parsedUri[1]" + parsedUri[1]);
            //  System.out.println("parsedUri[2]" + parsedUri[2]);
            //  System.out.println("parsedUri[3]" + parsedUri[3]);
            System.out.println("moleculesdatapath ==>" + config.getMoleculesdatapath() + "/" + parsedUri[3]);
            fileSource = config.getMoleculesdatapath() + "/" + parsedUri[3];
        }

       // System
        File file = new File(fileSource);
        try (FileInputStream fis = new FileInputStream(fileSource)) {
            // response.getOutputStream().write(data);

            //System.out.println("Configuration File");
            FileUtils.copyFile(file, response.getOutputStream());
            response.getOutputStream().flush();
            response.getOutputStream().close();
            //response.getWriter().print(json);
            fis.close();
        }
    }

    /**
     * Returns a short description of the servlet.
     *
     * @return a String containing servlet description
     */
    @Override
    public String getServletInfo() {
        return "Partitioning Servlet";
    }// </editor-fold>

}

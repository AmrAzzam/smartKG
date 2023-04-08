/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package QueryAnalyzer.Downloader;

import java.util.HashMap;
import java.util.List;

/**
 *
 * @author azzam
 */
public interface PartitionsDownloader {
        public void DownloadPartitionsByStars(HashMap<String, List<Integer>> querySolutionsPartitions);

    public void closeConnection();
}

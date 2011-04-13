/* This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 */
/**
 * 
 */
package edu.ku.brc.sgr;

import java.net.MalformedURLException;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.MoreLikeThisParams;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * A BatchMatcher instance handles matching a batch of records against a SOLR
 * index of potentially duplicate records.
 * 
 * Results are spooled into an instance of {@link BatchMatchResults} and can 
 * potentially be resumed. 
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 7, 2011
 *
 */

public class BatchMatcher
{
    final private CommonsHttpSolrServer server;
    final private String serverUrl;
    final private SolrQuery baseQuery;
    final private int nThreads;
    
    /**
     * Only instantiated by {@link BatchMatcher.Factory}
     */
    private BatchMatcher(final String url,
                       final int threads,
                       final int minDocFreq,
                       final int minTermFreq,
                       final boolean boostInterstingTerms,
                       final String similarityFields) 
        throws MalformedURLException {
        
        serverUrl = url;
        server = new CommonsHttpSolrServer(url);
        
        this.nThreads = threads;

        baseQuery = new SolrQuery();
        baseQuery.setQueryType("/" + MoreLikeThisParams.MLT);
        baseQuery.set(CommonParams.FL, "score");
        baseQuery.setRows(1);
        baseQuery.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
        baseQuery.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
        baseQuery.set(MoreLikeThisParams.BOOST, boostInterstingTerms);
        baseQuery.set(MoreLikeThisParams.SIMILARITY_FIELDS, similarityFields);
    }
    
    public BatchMatchResults match(final Iterable<Matchable> toMatch,
                                   final BatchMatchResults inResults) {
        // We are resuming if given a set of partial results.
        final boolean resuming = (inResults != null);
        
        final BatchMatchResults results;
        final ImmutableSet<String> completedIds;
        
        if (resuming) {
            // Make sure we are resuming a set of results with the same query.
            if (!inResults.getBaseQuery().toString().equals(
                    this.baseQuery.toString())) {
                        throw new IllegalArgumentException(
                            "cannot resume batchmatch with inconsistent query");
            }
            results = inResults;
            completedIds = results.getCompletedIds();
        } else {
            results = new BatchMatchResults(serverUrl, baseQuery);
            completedIds = ImmutableSet.of();
        }
        
        // Setup work unit for threads.
        final BlockingJobQueue.Worker<Matchable> worker = 
            new BlockingJobQueue.Worker<Matchable>() 
        {
            @Override
            public void doWork(Matchable toMatch)
            {
                if (resuming && completedIds.contains(toMatch.getId())) {
                    return;
                }
                final MatchResults result = toMatch.doMatch(server, baseQuery);
                results.addResult(result);
            }
        };
        
        final BlockingJobQueue<Matchable> jobs = 
            new BlockingJobQueue<Matchable>(nThreads, worker);

        
        // GO.
        jobs.startThreads();
        
        for (Matchable item: toMatch) { jobs.addWork(item); }
        
        jobs.waitForAllJobsToComplete();
        jobs.stopThreads();
        return results;        
    }
    

    /**
     * Get a mutable factory object which has public attributes to configure
     * a {@link BatchMatcher}.  Calling build() will return an immutable
     * {@link BatchMatcher} with the given setup. 
     */
    public static Factory getFactory() { return new Factory(); }
    
    public static class Factory {
        public String serverUrl = "http://localhost:8983/solr";
        public int nThreads = 4;
        public String idSuffix = "";
        public int minDocFreq = 1;
        public int minTermFreq = 1;
        public boolean boostInterestingTerms = true;
        public String[] similarityFields = {
                "collectors", 
                "collector_number", 
                "location", 
                "date_collected", 
                "date_split", 
                "scientific_name" 
                }; 
        
        public BatchMatcher build() throws MalformedURLException {
            return new BatchMatcher(serverUrl, nThreads,
                    minDocFreq, minTermFreq, boostInterestingTerms,
                    Joiner.on(',').join(similarityFields));
        }
    }
}

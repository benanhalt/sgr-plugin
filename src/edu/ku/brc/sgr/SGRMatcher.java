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


/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 12, 2011
 *
 */
public class SGRMatcher
{
    private final CommonsHttpSolrServer server;
    private final SolrQuery baseQuery;

    private SGRMatcher(final String url,
                       final int minDocFreq,
                       final int minTermFreq,
                       final boolean boostInterstingTerms,
                       final String similarityFields) 
          throws MalformedURLException {
          
          server = new CommonsHttpSolrServer(url);
          
          baseQuery = new SolrQuery();
          baseQuery.setQueryType("/" + MoreLikeThisParams.MLT);
          baseQuery.set(CommonParams.FL, "score");
          baseQuery.setRows(1);
          baseQuery.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
          baseQuery.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
          baseQuery.set(MoreLikeThisParams.BOOST, boostInterstingTerms);
          baseQuery.set(MoreLikeThisParams.SIMILARITY_FIELDS, similarityFields);
      }
    
    public static Factory getFactory() { return new Factory(); }
    
    public MatchResults match(Matchable matchable)
    {
        SolrQuery query = baseQuery.getCopy();
        return matchable.doMatch(server, query);
    }
    
    public static class Factory {
        public String serverUrl = "http://localhost:8983/solr";
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
        
        public SGRMatcher build() throws MalformedURLException {
            return new SGRMatcher(serverUrl, minDocFreq, minTermFreq, 
                    boostInterestingTerms, Joiner.on(',').join(similarityFields));
        }
    }    
}

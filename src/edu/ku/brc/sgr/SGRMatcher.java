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
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;

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
    
    public final String serverUrl;

    private SGRMatcher(final String url, final SolrQuery baseQuery) throws MalformedURLException 
    {
        serverUrl = url;  
        server = new CommonsHttpSolrServer(url);
        this.baseQuery = baseQuery;
    }
    
    public static Factory getFactory() { return new Factory(); }
    
    public SolrQuery getBaseQuery() { return baseQuery.getCopy(); }
    
    public MatchResults match(Matchable matchable)
    {
        return matchable.doMatch(server, getBaseQuery());
    }
    
    public boolean sameQueryAs(String query)
    {
        SolrParams params = SolrRequestParsers.parseQueryString(query);
        return EquateSolrParams.equals(getBaseQuery(), params);
    }
    
    public static class Factory {
        public String serverUrl = "http://localhost:8983/solr";
        public int nRows = 10;
        public int minDocFreq = 1;
        public int minTermFreq = 1;
        public boolean boostInterestingTerms = true;
        public boolean docSupplied = false;
        public String similarityFields = 
            "collectors,collector_number,location,date_collected,date_split,scientific_name"; 
        public String returnedFields = "*,score";
        public String queryFields = "";
        public String filterQuery = "";
        
        public SGRMatcher build() throws MalformedURLException {
            SolrQuery baseQuery = new SolrQuery();
            baseQuery.setQueryType("/" + MoreLikeThisParams.MLT);
            baseQuery.set(CommonParams.FL, returnedFields);
            baseQuery.setRows(nRows);
            baseQuery.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
            baseQuery.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
            baseQuery.set(MoreLikeThisParams.BOOST, boostInterestingTerms);
            baseQuery.set(MoreLikeThisParams.DOC_SUPPLIED, docSupplied);
            baseQuery.set(MoreLikeThisParams.SIMILARITY_FIELDS, similarityFields);
            baseQuery.set(MoreLikeThisParams.QF, queryFields);
            baseQuery.set(MoreLikeThisParams.PRESERVE_FIELDS, true);
            baseQuery.set(CommonParams.FQ, filterQuery);
            
            return new SGRMatcher(serverUrl, baseQuery);
        }
    }    
}

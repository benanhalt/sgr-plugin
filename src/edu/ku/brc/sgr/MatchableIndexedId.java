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

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

import com.google.common.collect.ImmutableList;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 13, 2011
 *
 */
public class MatchableIndexedId implements Matchable
{
    final public String id;
    
    public MatchableIndexedId(String id)
    {
        this.id = id;
    }

    /* (non-Javadoc)
     * @see edu.ku.brc.sgr.Matchable#getId()
     */
    @Override
    public String getId()
    {
        return id;
    }

    /* (non-Javadoc)
     * @see edu.ku.brc.sgr.Matchable#doMatch(org.apache.solr.client.solrj.SolrServer, org.apache.solr.client.solrj.SolrQuery)
     */
    @Override
    public MatchResults doMatch(SolrServer server, SolrQuery baseQuery)
    {
        final SolrQuery query = baseQuery.getCopy();
        query.setQuery("id:" + id);

        final QueryResponse resp;
        try { resp = server.query(query); } 
        catch (SolrServerException e) {
            // TODO Auto-generated catch block
            throw new RuntimeException(e);
        }        
        
        final SolrDocumentList docs = resp.getResults();
        final float maxScore = (docs != null) ? docs.getMaxScore() : 0.0f;
        final ImmutableList.Builder<Match> msBuilder = ImmutableList.builder();
        if (docs != null)
        {
            for (SolrDocument doc: docs)
            {
                float score = (Float) doc.getFieldValue("score");
                SGRRecord match = SGRRecord.fromSolrDocument(doc);
                msBuilder.add(new Match(match, score));
            }
        }
        return new MatchResults(id, resp.getQTime(), maxScore, msBuilder.build());        
    }
}

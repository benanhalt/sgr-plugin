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

import java.util.LinkedList;
import java.util.List;

import org.apache.solr.client.solrj.SolrQuery;

import com.google.common.collect.ImmutableSet;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 8, 2011
 *
 */
public class BatchMatchResults implements BatchMatchResultAccumulator
{
    final private SolrQuery baseQuery;
    final public String serverUrl;
    
    final private List<MatchResults> matches = new LinkedList<MatchResults>();
    
    public BatchMatchResults(final String serverUrl,
                             final SolrQuery basequery) {
        this.baseQuery = basequery.getCopy();
        this.serverUrl = serverUrl;
    }
    
    @Override
    synchronized public void addResult(final MatchResults result) {
        matches.add(result);
    }
    
    @Override
    synchronized public ImmutableSet<String> getCompletedIds() {
        final ImmutableSet.Builder<String> completed = ImmutableSet.builder();
        for (MatchResults r: matches) { completed.add(r.matchedId); }
        return completed.build();
    }
    
    @Override
    public SolrQuery getBaseQuery() {
        return baseQuery.getCopy();
    }
}

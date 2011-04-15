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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Set;

import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.servlet.SolrRequestParsers;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 15, 2011
 *
 */
public class BatchMatchResultsInFile implements BatchMatchResultAccumulator
{
    public final boolean resuming;
    
    private final SolrParams solrParams;
    private final Set<String> completedIds;
    private PrintWriter output;
    
    public BatchMatchResultsInFile(String fileName, SGRMatcher matcher) throws IOException
    {
        final File file = new File(fileName);

        BufferedReader in = null;
        try {
            in = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {}

        resuming = (in != null);
        
        solrParams = resuming ?
                SolrRequestParsers.parseQueryString(in.readLine())
                :
                matcher.getBaseQuery();        

        completedIds = Sets.newHashSet();
        if (resuming)
        {
            while (true)
            {
                final String line = in.readLine();
                if (line == null) break;
                final String[] fields = line.split("\t");
                completedIds.add(fields[0]);
            }
            in.close();
        }

        output = new PrintWriter(new FileWriter(file, true), true);
        
        if (!resuming)
        {
            output.println(matcher.getBaseQuery().toString());  
        }
    }
    
    public void close()
    {
        output.close();
    }

    /* (non-Javadoc)
     * @see edu.ku.brc.sgr.BatchMatchResultAccumulator#addResult(edu.ku.brc.sgr.MatchResults)
     */
    @Override
    synchronized public void addResult(MatchResults result)
    {
        completedIds.add(result.matchedId);
        output.println(result.matchedId + "\t" + result.qTime + "\t" + result.maxScore);        
    }

    /* (non-Javadoc)
     * @see edu.ku.brc.sgr.BatchMatchResultAccumulator#getCompletedIds()
     */
    @Override
    public ImmutableSet<String> getCompletedIds()
    {
        return ImmutableSet.copyOf(completedIds);
    }

    /* (non-Javadoc)
     * @see edu.ku.brc.sgr.BatchMatchResultAccumulator#getBaseQuery()
     */
    @Override
    public ModifiableSolrParams getBaseQuery()
    {
        return new ModifiableSolrParams(solrParams);
    }

    @Override
    public int nCompleted()
    {
        return completedIds.size();
    }
}

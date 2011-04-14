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

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.solr.client.solrj.SolrQuery;

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
    private final SGRMatcher matcher;
    public final int nThreads;

    private final BlockingJobQueue<Matchable> jobs;
    private final Iterable<Matchable> items;
    
    private boolean used = false;
    private final AtomicInteger nItemsSkipped = new AtomicInteger();

    public BatchMatcher(final SGRMatcher matcher, final Iterator<? extends Matchable> toMatch,
                        final BatchMatchResultAccumulator accumulator, int nThreads)
    {
        this.matcher = matcher;
        this.nThreads = nThreads;

        final ImmutableSet<String> completedIds = accumulator.getCompletedIds();
        final boolean resuming = (completedIds.size() > 0); 
        
        // Make sure accumaltor's stored query matches what we are using now.
        if (!accumulator.getBaseQuery().toString().equals(
                matcher.getBaseQuery().toString())) {
                    throw new IllegalArgumentException(
                        "cannot resume batchmatch with inconsistent query");
        }

        // Setup work unit for threads.
        final BlockingJobQueue.Worker<Matchable> worker = 
            new BlockingJobQueue.Worker<Matchable>() 
        {
            @Override
            public void doWork(Matchable toMatch)
            {
                if (resuming && completedIds.contains(toMatch.getId())) {
                    nItemsSkipped.getAndIncrement();
                    return;
                }
                accumulator.addResult(matcher.match(toMatch));
            }
        };
        
        jobs = new BlockingJobQueue<Matchable>(nThreads, worker);

        items = new Iterable<Matchable>()
        {
            @Override
            public Iterator<Matchable> iterator() { return (Iterator<Matchable>) toMatch; }
        };
    }
    
    public void run()
    {
        if (used)
        {
            throw new IllegalStateException("BatchMatcher instance can only be used once.");
        }
        used = true;
        
        jobs.startThreads();
        
        for (Matchable item: items)
        { 
            jobs.addWork(item); 
        }
        
        jobs.waitForAllJobsToComplete();
        jobs.stopThreads();
    }
    
    public SolrQuery getBaseQuery()
    {
        return matcher.getBaseQuery();
    }
    
    public int getTotalCurrentlyQueued()
    {
        return jobs.getCurrentJobsQueued();
    }
    
    public int getTotalQueued()
    {
        return jobs.getTotalJobsQueued();
    }
    
    public int getTotalFinished()
    {
        return jobs.getTotalJobsFinished();
    }
    
    public int getTotalSkipped()
    {
        return nItemsSkipped.get();
    }
    
    public static SGRMatcher.Factory getMatcherFactory()
    {
        SGRMatcher.Factory factory = SGRMatcher.getFactory();
        factory.nRows = 1;
        factory.returnedFields = "id,score";
        return factory;
    }
}

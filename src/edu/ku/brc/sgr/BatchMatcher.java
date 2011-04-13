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
    
    public BatchMatcher(SGRMatcher matcher, int nThreads)
    {
        this.matcher = matcher;
        this.nThreads = nThreads;
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
                    matcher.getBaseQuery().toString())) {
                        throw new IllegalArgumentException(
                            "cannot resume batchmatch with inconsistent query");
            }
            results = inResults;
            completedIds = results.getCompletedIds();
        } else {
            results = new BatchMatchResults(matcher.serverUrl, 
                                            matcher.getBaseQuery());
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
                results.addResult(matcher.match(toMatch));
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
}

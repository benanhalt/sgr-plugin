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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Mar 14, 2011
 *
 */
public class BlockingJobQueue<T>
{
    private final BlockingQueue<T> workQueue = new SynchronousQueue<T>();
    private final AtomicInteger currentJobsQueued = new AtomicInteger();
    private final AtomicInteger totalJobsQueued = new AtomicInteger();
    private final AtomicInteger totalJobsFinished = new AtomicInteger();
    private final Worker<T> worker;
    private final List<Thread> threads;
    private final ExceptionHandler<T> exceptionHandler;
    
    private Exception abortException = null;
    
    public interface Worker<T>
    {
        public void doWork(T work);
    }
    
    public interface ExceptionHandler<T>
    {
        public Exception handle(T work, Exception e);
    }

    public BlockingJobQueue(final int nThreads, final Worker<T> processWork)
    {
        this(nThreads, processWork, new ExceptionHandler<T>()
        {
            @Override
            public Exception handle(T work, Exception e)
            {
                return e;
            }
        });
        
    }
    
    public BlockingJobQueue(final int nThreads, final Worker<T> processWork,
                            final ExceptionHandler<T> exceptionHandler)
    {
        this.worker = processWork;
        this.exceptionHandler = exceptionHandler;
        
        threads = new ArrayList<Thread>(nThreads);
        for (int i = 0; i < nThreads; i++)
        {
            final Thread t = new Thread(new Runnable()
            {
                @Override
                public void run() { doWork(); }
            });
            threads.add(t);
        }
    }
    
    public void startThreads()
    {
        for (Thread t : threads)
        {
            t.start();
        }
    }
    
    public void stopThreads()
    {
        for (Thread t : threads)
        {
            t.interrupt();
        }            
    }
    
    public void addWork(final T id) throws Exception
    {
        if (abortException != null)
        {
            throw abortException;
        }
        
        currentJobsQueued.incrementAndGet();
        workQueue.put(id);
        totalJobsQueued.incrementAndGet();
    }
    
    synchronized public void waitForAllJobsToComplete()
    {
        if (currentJobsQueued.get() == 0) return;
        
        while (true)
        {
            try
            {
                this.wait();
                if (currentJobsQueued.get() == 0) return;
            } catch (InterruptedException e) {}
        }
    }
    
    public int getCurrentJobsQueued()
    {
    	return currentJobsQueued.get();
    }
    
    public int getTotalJobsQueued()
    {
    	return totalJobsQueued.get();
    }
    
    public int getTotalJobsFinished()
    {
    	return totalJobsFinished.get();
    }
    
    private void doWork()
    {
        while (true)
        {
            try
            {
                T work = workQueue.take();
                try { worker.doWork(work); }
                catch (Exception e)
                {
                    abortException = exceptionHandler.handle(work, e);
                }
                finishedJob();
            } catch (InterruptedException e)
            {
                // TODO: Should the work be requeued?
                return;
            }
        }
    }
    
    synchronized private void finishedJob()
    {
    	totalJobsFinished.incrementAndGet();
        if(currentJobsQueued.decrementAndGet() == 0)
        {
            this.notifyAll();
        }
    }

}

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
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 14, 2011
 *
 */
public interface BatchMatchResultAccumulator
{
    void addResult(final MatchResults result);
    ImmutableSet<String> getCompletedIds();
    int nCompleted();
    SGRMatcher getMatcher();
}

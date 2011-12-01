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

import java.util.Arrays;

import org.apache.solr.common.params.SolrParams;

import com.google.common.collect.ImmutableSet;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 15, 2011
 *
 */
public class EquateSolrParams
{
    public static boolean equals(SolrParams a, SolrParams b)
    {
        ImmutableSet<String> aParamNames = ImmutableSet.copyOf(a.getParameterNamesIterator());
        ImmutableSet<String> bParamNames = ImmutableSet.copyOf(b.getParameterNamesIterator());
        
        if (!aParamNames.equals(bParamNames))
            return false;
        
        for (String name: aParamNames)
        {
            if (!Arrays.equals(a.getParams(name), b.getParams(name)))
                return false;
        }
        
        return true;
    }
}

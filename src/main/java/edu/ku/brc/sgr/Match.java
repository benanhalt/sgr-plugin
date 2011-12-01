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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @author ben
 *
 * @code_status Alpha
 *
 * Created Date: Apr 11, 2011
 *
 */
public class Match
{
    public final SGRRecord match;
    public final float score;
    public final String explained;
    
    private ImmutableMap<String, Float> _fieldScoreContributions = null;
    
    private static final Pattern explainFieldRegexp = Pattern.compile(
            "([^\\s]+) = \\(MATCH\\) weight\\(([^\\:]+)\\:([^\\s]+)");
    
    public Match(SGRRecord match, float score) {
        this(match, score, null);
    }

    public Match(SGRRecord match, float score, String explained)
    {
        this.match = match;
        this.score = score;
        if (explained == null) explained = "";
        this.explained = explained;
    }
    
    public ImmutableMap<String, Float> fieldScoreContributions()
    {
        if (_fieldScoreContributions == null)
        {
            Map<String, Float> data = Maps.newHashMap();
            Matcher matcher = explainFieldRegexp.matcher(explained);
            while (matcher.find())
            {
                String contribution = matcher.group(1);
                String field = matcher.group(2);
                
                Float cur = data.get(field);
                if (cur == null) cur = 0.0f;
                data.put(field, cur + Float.parseFloat(contribution));
            }
            _fieldScoreContributions = ImmutableMap.copyOf(data);
        }
            
        return _fieldScoreContributions;
    }
}

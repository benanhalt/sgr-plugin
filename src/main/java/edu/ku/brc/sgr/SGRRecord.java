/*
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 * 
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 * 
 * You should have received a copy of the GNU Lesser General Public License along with this library;
 * if not, write to the Free Software Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA
 * 02111-1307 USA
 */
/**
 * 
 */
package edu.ku.brc.sgr;

import java.util.Map;

import org.apache.solr.common.SolrDocument;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;

/**
 * @author ben
 * 
 * @code_status Alpha
 * 
 * Created Date: Apr 11, 2011
 * 
 */
public class SGRRecord
{
    public final String id;
    
    private final ImmutableListMultimap<String, String> data;

    public SGRRecord(String id, Multimap<String, String> data)
    {
        this.id = id;
        this.data = ImmutableListMultimap.copyOf(data);
    }

    public ImmutableListMultimap<String, String> getMultimap()
    {
        return data;
    }

    public ImmutableSet<String> getFields()
    {
        return data.keySet();
    }

    public ImmutableList<String> getFieldValues(String field)
    {
        return data.get(field);
    }
    
    public Matchable asMatchable()
    {
        return MatchableRecord.from(this);
    }

    public SolrDocument asSolrDocument()
    {
        SolrDocument doc = new SolrDocument();
        doc.addField("id", id);
        for (Map.Entry<String, String> e : data.entries())
        {
            doc.addField(e.getKey(), e.getValue());
        }
        return doc;
    }

    public static SGRRecord fromSolrDocument(SolrDocument doc)
    {
        final String id = (String) doc.getFieldValue("id");
        final ImmutableListMultimap.Builder<String, String> dataBuilder 
            = ImmutableListMultimap.builder();
        
        for (String fieldName: doc.getFieldNames())
        {
            if (fieldName.equals("score")) continue;
            if (fieldName.equals("id")) continue;
            
            for (Object v: doc.getFieldValues(fieldName)) 
            {
                dataBuilder.put(fieldName, v.toString());
            }
        }
        return new SGRRecord(id, dataBuilder.build());
    }
    
    public static Builder builder(String id)
    {
        return new Builder(id);
    }
    
    public static class Builder 
    {
        private final ImmutableListMultimap.Builder<String, String> builder =
            ImmutableListMultimap.builder();
        public final String id;
        
        public Builder(String id)
        {
            this.id = id;
        }
        
        public Builder put(String field, String value)
        {
            builder.put(field, value);
            return this;
        }
        
        public SGRRecord build()
        {
            return new SGRRecord(id, builder.build());
        }
    }

}

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.myhadoop.pig;

import com.sun.xml.internal.ws.api.pipe.Tube;
import java.io.IOException;
import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

/**
 *
 * @author jw362j
 * 
 * http://www.tuicool.com/articles/N32UVb2
 * 
 * 
 * http://pig.apache.org/docs/r0.12.1/udf.html
 * 
 */
public class IsValidFunction  extends FilterFunc{

    @Override
    public Boolean exec(Tuple tuple) throws IOException {
        final Integer id = (Integer) tuple.get(0);
        final Integer age = (Integer)tuple.get(1);
        return  id.intValue() == age.intValue();
    }
    
}

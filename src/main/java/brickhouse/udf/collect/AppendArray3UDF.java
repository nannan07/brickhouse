package brickhouse.udf.collect;
/**
 * Copyright 2012 Klout, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 **/


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;

import java.util.List;


/**
 * Return a list of unique entries, for a given set of lists.
 * <p/>
 * {1, 2} append 3 = {1, 2,3}
 * {1, 2} append 2 = {1, 2, 2}
 */

@Description(name = "append_array3",
        value = "_FUNC_(array, object) - Returns the union of a  arrays "
)
public class AppendArray3UDF extends GenericUDF {
    private StandardListObjectInspector retValInspector;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {
        List retVal = retValInspector.getList(arg0[0].get());
        if (retVal.size() != 3) {
            throw new UDFArgumentException(" first array arg Expecting three elements");
        }
        if (retVal.get(2) != null) {
            retVal.set(1, retVal.get(2));
        }
        retVal.set(2, arg0[1].get());
        return retVal;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "append_array3(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 2) {
            throw new UDFArgumentException(" Expecting  two  arguments ");
        }
        if (arg0[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException(" Expecting an array as first argument ");
        }
        ListObjectInspector first = (ListObjectInspector) arg0[0];

        ObjectInspector second = arg0[1];
        if (!ObjectInspectorUtils.compareTypes(first.getListElementObjectInspector(), second)) {
            throw new UDFArgumentException(" Array types must match~~~~~:: " + first.getTypeName() + " != " + second.getTypeName());
        }
        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);

        return retValInspector;
    }

}

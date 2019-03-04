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
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

import java.util.List;


/**
 * Return a list of unique entries, for a given set of lists.
 * <p/>
 * {1, 2} append 3 = {1, 2, 3}
 * {1, 2} append 2 = {1, 2, 2}
 */

@Description(name = "append_array6",
        value = "_FUNC_(array, object) - Returns the union of a  arrays "
)
public class AppendArray6UDF extends GenericUDF {
    private StandardListObjectInspector retValInspector;
    //    private PrimitiveObjectInspector listElemInspector;
    private boolean returnWritables;
    private PrimitiveObjectInspector primInspector;

    @Override
    public Object evaluate(DeferredObject[] args) throws HiveException {
        List objList = retValInspector.getList(args[0].get());

        Object objToAppend = args[1].get();
        Object[] res = new Object[objList.size() + 1];
        for (int i = 0; i < objList.size(); i++) {
            Object o = objList.get(i);
            res[i] = returnWritables ?
                    primInspector.getPrimitiveWritableObject(o) :
                    primInspector.getPrimitiveJavaObject(o);
        }
        res[res.length - 1] = returnWritables ?
                primInspector.getPrimitiveWritableObject(objToAppend) :
                primInspector.getPrimitiveJavaObject(objToAppend);
        return res;
    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "append_array6(" + arg0[0] + ", " + arg0[1] + " )";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 2) {
            throw new UDFArgumentException(" Expecting  two  arguments ");
        }
        if (args[0].getCategory() != Category.LIST) {
            throw new UDFArgumentException(" Expecting an array as first argument ");
        }
        ListObjectInspector first = (ListObjectInspector) args[0];

        ObjectInspector second = args[1];
        if (!ObjectInspectorUtils.compareTypes(first.getListElementObjectInspector(), second)) {
            throw new UDFArgumentException(" Array types must match " + first.getTypeName() + " != " + second.getTypeName());
        }
//        listElemInspector = (PrimitiveObjectInspector) first.getListElementObjectInspector();
        System.out.println("mytest:" + second.getTypeName());

        primInspector = (PrimitiveObjectInspector) second;

        returnWritables = primInspector.preferWritable();

        retValInspector = (StandardListObjectInspector) ObjectInspectorUtils.getStandardObjectInspector(first);
        return retValInspector;
    }

}

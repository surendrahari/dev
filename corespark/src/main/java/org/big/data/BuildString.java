package org.big.data;

import org.apache.spark.api.java.function.MapFunction;

/**
 * Created by hadoop on 4/24/17.
 */
public class BuildString
        implements MapFunction<JoinEmpDept, String> {
    public String call (JoinEmpDept u) throws Exception {
        return u.getNo() + " : " + u.getName() + " : " + u.getSal() + " : " + u.getSal() + " : " + u.getDeptname();
    }
}

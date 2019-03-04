package brickhouse.udf.date;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;


public class FirstDayOfWeekUDF extends UDF {
    private static final DateTimeFormatter YYYYMMDD = org.joda.time.format.DateTimeFormat.forPattern("YYYY-MM-dd");

    //获取指定日期所在周的第一天的日期
    public String evaluate(String dateStr) {
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        return dt.dayOfWeek().withMinimumValue().toString(YYYYMMDD);
    }


    //获取当前时间所在周的第一天的日期
    public String evaluate() {
        DateTime dt = DateTime.now();
        return dt.dayOfWeek().withMinimumValue().toString(YYYYMMDD);
    }
    //获取指定日期所在周的第一天的日期,距离该日期第numWeeks周后的第一天日期。numWeeks可为正数或者负数，比如-1表示上一周的第一天的日期
    public String evaluate(String dateStr, int numWeeks) {
        DateTime dt = YYYYMMDD.parseDateTime(dateStr);
        DateTime addedDt = dt.dayOfWeek().withMinimumValue().plusWeeks(numWeeks);
        return addedDt.toString(YYYYMMDD);
    }

    public static void main(String[] args) {
        FirstDayOfWeekUDF udf = new FirstDayOfWeekUDF();
        System.out.println(udf.evaluate("2016-11-06"));
        System.out.println(udf.evaluate());
        System.out.println(udf.evaluate("2016-11-06",-1));
    }
}

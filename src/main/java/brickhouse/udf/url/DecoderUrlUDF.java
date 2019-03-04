package brickhouse.udf.url;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.net.URLDecoder;

@Description(name = "decoder_url", value = "_FUNC_(url [,code][,count]) - decoder a URL from a String for count times using code as encoding scheme ", extended = ""
        + "if count is not given ,the url will be decoderd for 2 time,"
        + "if code is not given ,UTF-8 is used")
public class DecoderUrlUDF extends UDF {
    private String url = null;
    private int times = 2;
    private String code = "UTF-8";

    public DecoderUrlUDF() {
    }

    public String evaluate(String urlStr, String srcCode, int count) {
        if (urlStr == null) {
            return null;
        }
        if (count <= 0) {
            return urlStr;
        }
        if (srcCode != null) {
            code = srcCode;
        }
        url = urlStr;
        times = count;
        for (int i = 0; i < times; i++) {
            url = decoder(url, code);
        }
        return url;
    }

    public String evaluate(String urlStr, String srcCode) {
        if (urlStr == null) {
            return null;
        }
        url = urlStr;
        code = srcCode;
        return evaluate(url, code, times);
    }

    public String evaluate(String urlStr, int count) {
        if (urlStr == null) {
            return null;
        }
        if (count <= 0) {
            return urlStr;
        }
        url = urlStr;
        times = count;

        return evaluate(url, code, times);
    }

    public String evaluate(String urlStr) {
        if (urlStr == null) {
            return null;
        }
        url = urlStr;
        return evaluate(url, code, times);
    }

    private String decoder(String urlStr, String code) {
        if (urlStr == null || code == null) {
            return null;
        }
        try {
            urlStr = URLDecoder.decode(urlStr, code);
        } catch (Exception e) {
            return null;
        }
        return urlStr;
    }

    public static void main(String[] args) {
        System.out.println(new DecoderUrlUDF().evaluate("%E5%8F%AA%E6%B1%82%E4%B8%AD"));
        System.out.println("2692D4CF-3F33-413F-BFFF-D79897CEEA90".hashCode() % 5);
    }
}
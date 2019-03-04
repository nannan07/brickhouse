package brickhouse.udf.mysql;


import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@Description(name = "mysql_batch_import",
        value = "_FUNC_(config_path, sql,array<struct>) - Return ret "
)
public class MysqlBatchImportUDF extends GenericUDF {

    public static final String DEFAULT_CONFIG_ROOT_PATH = "/user/hive/udf/mysqludf/";
    public static final String DEFAULT_CONFIG_FILE_SUFFIX = "properties";
    private StandardListObjectInspector retValInspector;
    private Properties properties;
    private String sql;
    private StandardListObjectInspector paramsListInspector;
    private StandardStructObjectInspector paramsElementInspector;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {

        //batch import由于是一次性插入，所以不创建连接池了，直接创建一个连接执行
        try (Connection connection = DriverManager.getConnection(properties.getProperty("url"), properties.getProperty("username"), properties.getProperty("password"));
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            connection.setAutoCommit(false);

            for (int i = 0; i < paramsListInspector.getListLength(arg0[2].get()); i++) {
                Object row = paramsListInspector.getListElement(arg0[2].get(), i);
                for (int j = 0; j < paramsElementInspector.getAllStructFieldRefs().size(); j++) {
                    StructField structField = paramsElementInspector.getAllStructFieldRefs().get(j);
                    Object col = paramsElementInspector.getStructFieldData(row, structField);
                    Object param = ((PrimitiveObjectInspector) structField.getFieldObjectInspector()).getPrimitiveJavaObject(col);
                    stmt.setObject(j + 1, param);
                }
                stmt.addBatch();
            }
            int[] ret = stmt.executeBatch();
            connection.commit();

            Object returnlist = retValInspector.create(ret.length);
            for (int i = 0; i < ret.length; i++) {
                retValInspector.set(returnlist, i, ret[i]);
            }
            return returnlist;

        } catch (SQLException e) {
            e.printStackTrace();
            throw new HiveException(e);
        }

    }


    @Override
    public String getDisplayString(String[] arg0) {
        return "mysql_batch_import(config_path, sql,array<struct>)";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length != 3) {
            throw new UDFArgumentException(" Expecting   three  arguments ");
        }
        //第一个参数校验
        if (arg0[0].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[0]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[0] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("mysql connection pool config path  must be constant");
            }
            ConstantObjectInspector propertiesPath = (ConstantObjectInspector) arg0[0];

            String fileName1 = propertiesPath.getWritableConstantValue().toString();
            Path path1 = new Path(fileName1);
            if (path1.toUri().getScheme() == null) {
                if (!"".equals(FilenameUtils.getExtension(fileName1)) && !DEFAULT_CONFIG_FILE_SUFFIX.equals(FilenameUtils.getExtension(fileName1))) {
                    throw new UDFArgumentException("不支持的文件扩展名，目前只支持properties文件!");
                }
                //如果是相对路径,补齐根路径
                if (!fileName1.startsWith("/")) {
                    fileName1 = MysqlBatchImportUDF.DEFAULT_CONFIG_ROOT_PATH + fileName1;
                }
            }
            //如果只写了文件前缀的话，补上后缀
            if (!FilenameUtils.isExtension(fileName1, DEFAULT_CONFIG_FILE_SUFFIX)) {
                fileName1 = fileName1 + FilenameUtils.EXTENSION_SEPARATOR_STR + DEFAULT_CONFIG_FILE_SUFFIX;
            }
            Properties properties = new Properties();
            Configuration conf = new Configuration();
            Path path2 = new Path(fileName1);

            try (FileSystem fs = FileSystem.newInstance(path2.toUri(), conf); //这里不能用FileSystem.get(path2.toUri(), conf)，必须得重新newInstance,get出来的是共享的连接，这边关闭的话，会导致后面执行完之后可能出现FileSystem is closed的异常
                 InputStream in = fs.open(path2)) {
                properties.load(in);
                this.properties = properties;
            } catch (FileNotFoundException ex) {
                throw new UDFArgumentException("在文件系统中或者是HDFS上没有找到对应的配置文件");
            } catch (Exception e) {
                e.printStackTrace();
                throw new UDFArgumentException(e);
            }
        }
        //第二个参数校验，必须是一个非空的sql语句
        if (arg0[1].getCategory() == Category.PRIMITIVE
                && ((PrimitiveObjectInspector) arg0[1]).getPrimitiveCategory() == PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            if (!(arg0[1] instanceof ConstantObjectInspector)) {
                throw new UDFArgumentException("the second arg   must be a sql string constant");
            }
            ConstantObjectInspector sqlInsp = (ConstantObjectInspector) arg0[1];
            this.sql = sqlInsp.getWritableConstantValue().toString();
            if (this.sql == null || this.sql.trim().length() == 0) {
                throw new UDFArgumentException("the second arg   must be a sql string constant and not nullable");
            }
        }

        //第三个参数校验
        if (arg0[2].getCategory() != Category.LIST) {
            throw new UDFArgumentException(" Expecting an array<struct> field as third argument ");
        }
        ListObjectInspector third = (ListObjectInspector) arg0[2];
        if (third.getListElementObjectInspector().getCategory() != Category.STRUCT) {
            throw new UDFArgumentException(" Expecting an array<struct> field as third argument ");
        }
        paramsListInspector = ObjectInspectorFactory.getStandardListObjectInspector(third.getListElementObjectInspector());
        paramsElementInspector = (StandardStructObjectInspector) third.getListElementObjectInspector();
        retValInspector = ObjectInspectorFactory.getStandardListObjectInspector(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

        return retValInspector;
    }

}

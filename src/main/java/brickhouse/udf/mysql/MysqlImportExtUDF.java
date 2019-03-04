package brickhouse.udf.mysql;


import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

@Description(name = "mysql_import_ext",
        value = "_FUNC_(config_path, sql,args1,[args2,...]) - Return ret "
)
public class MysqlImportExtUDF extends GenericUDF {

    public static final String DEFAULT_CONFIG_ROOT_PATH = "/user/hive/udf/mysqludf/";
    public static final String DEFAULT_CONFIG_FILE_SUFFIX = "properties";

    private IntObjectInspector retValInspector;
    private DataSource dataSource;
    private String sql;
    private PrimitiveObjectInspector[] paramsInspectors;

    @Override
    public Object evaluate(DeferredObject[] arg0) throws HiveException {

        try (Connection connection = dataSource.getConnection();
             PreparedStatement stmt = connection.prepareStatement(sql)) {
            System.out.println("execute sql:" + System.currentTimeMillis());
            for (int i = 2; i < arg0.length; i++) {
                Object param = paramsInspectors[i - 2].getPrimitiveJavaObject(arg0[i].get());
                stmt.setObject(i - 1, param);
            }
            int ret = stmt.executeUpdate();
            IntWritable iw = new IntWritable(ret);

            return retValInspector.getPrimitiveWritableObject(iw);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new HiveException(e);
        }

    }


    @Override
    public void close() throws IOException {
        try {
            BasicDataSource bds = (BasicDataSource) dataSource;
            bds.close();
        } catch (SQLException e) {
            e.printStackTrace();
            throw new IOException(e);
        }
    }

    @Override
    public String getDisplayString(String[] arg0) {
        return "mysql_import(config_path, sql,args1[,args2,...argsN])";
    }


    @Override
    public ObjectInspector initialize(ObjectInspector[] arg0)
            throws UDFArgumentException {
        if (arg0.length < 3) {
            throw new UDFArgumentException(" Expecting  at least three  arguments ");
        }
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
                    fileName1 = MysqlImportExtUDF.DEFAULT_CONFIG_ROOT_PATH + fileName1;
                }
            }
            //如果只写了文件前缀的话，补上后缀
            if (!FilenameUtils.isExtension(fileName1, DEFAULT_CONFIG_FILE_SUFFIX)) {
                fileName1 = fileName1 + FilenameUtils.EXTENSION_SEPARATOR_STR + DEFAULT_CONFIG_FILE_SUFFIX;
            }
            Properties properties = new Properties();
            Configuration conf = new Configuration();
            Path path2 = new Path(fileName1);
            try (FileSystem fs = FileSystem.newInstance(path2.toUri(), conf);
                 InputStream in = fs.open(path2)) {

                properties.load(in);
                this.dataSource = BasicDataSourceFactory.createDataSource(properties);
            } catch (FileNotFoundException ex) {
                throw new UDFArgumentException("在文件系统中或者是HDFS上没有找到对应的配置文件");
            } catch (Exception e) {
                e.printStackTrace();
                throw new UDFArgumentException(e);
            }
        }
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
        paramsInspectors = new PrimitiveObjectInspector[arg0.length - 2];
        for (int i = 2; i < arg0.length; i++) {
            paramsInspectors[i - 2] = (PrimitiveObjectInspector) arg0[i];
        }
        retValInspector = PrimitiveObjectInspectorFactory.writableIntObjectInspector;

        return retValInspector;
    }

}

package brickhouse.udf.mysql;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.sql.DataSource;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

/**
 * Created by xiaoj on 2016/11/29.
 */
public class MyApp {
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        Configuration conf = new Configuration();
        Path path = new Path("hdfs://name84:8020/user/hive/udf/conf/mysql.properties");
        try (FileSystem fs = FileSystem.get(path.toUri(), conf);
             InputStream in = fs.open(path)) {

            properties.load(in);
            DataSource dataSource = BasicDataSourceFactory.createDataSource(properties);
//                  dataSource.getConnection()
            try (Connection connection = dataSource.getConnection();
                 Statement stmt = connection.createStatement();
                 ResultSet rset = stmt.executeQuery("select * from xyzs_streaming_xyzs_pc_stat")) {
                System.out.println("executeQuery Results:");
                int numcols = rset.getMetaData().getColumnCount();

                while (rset.next()) {
                    for (int i = 1; i <= numcols; i++) {
                        System.out.print("\t" + rset.getString(i));
                    }
                    System.out.println("");
                }
                System.out.println("Results display done.");
            }
            shutdownDataSource(dataSource);

        } catch (FileNotFoundException ex) {
            throw new RuntimeException("在文件系统中或者是HDFS上没有找到对应的配置文件");
        }
    }

    /**
     * @param ds
     * @throws SQLException
     */
    public static void shutdownDataSource(DataSource ds) throws SQLException {
        BasicDataSource bds = (BasicDataSource) ds;
        bds.close();
    }
}

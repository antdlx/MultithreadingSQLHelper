import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 不适用大文件的读取，实用性一般
 */

/**
 * Created by antdlx on 2016/11/1.
 * sql操作的工具类，主要包括了：
 * 连接数据库
 * 读取并解析sql文件
 * 执行sql语句和集合的各种方法
 */
public class SQLTools {

    /**
     * 一共开多少个线程
     */
    public static final int total_thread_num = 5;
    /**
     * 每个事务；一次性提交多少操作
     */
    public static final int per_commit_num = 100;
    /**
     * 默认把sql文件开头的非insert语句在主线程中执行，因为开头的几句sql语句涉及表的构建，防止子线程执行的时候表还不存在
     */
    public static final int head_sql_num = 10;

    /**
     * 读取并解析sql文件，把需要执行的命令放到一个List中
     * @param sqlFile sql文件的文件路径
     * @return 返回一个list，里面是需要执行的sql命令
     * @throws Exception
     */
    public static List<String> loadSql(String sqlFile) throws Exception {
        List<String> sqlList = new ArrayList<>();
        try {
            InputStream sqlFileIn = new FileInputStream(sqlFile);
            StringBuffer sqlSb = new StringBuffer();
            byte[] buff = new byte[1024];
            int byteRead = 0;
            while ((byteRead = sqlFileIn.read(buff)) != -1) {
                sqlSb.append(new String(buff, 0, byteRead));
            }

            // Windows 下换行是 \r\n, Linux 下是 \n,\s为空白符
            String[] sqlArr = sqlSb.toString()
                    .split("(;\\s*\\r\\n)|(;\\s*\\n)");
            for (int i = 0; i < sqlArr.length; i++) {
                String sql = sqlArr[i].replaceAll("--.*", "").trim();
                if (!sql.equals("")) {
                    sql = sql+";";
                    sqlList.add(sql);
                }
            }
            if (sqlList.size()>0){
                sqlList.set(0,"SET FOREIGN_KEY_CHECKS=0;");
            }
            return sqlList;
        } catch (Exception ex) {
            throw new Exception(ex.getMessage());
        }
    }

    /**
     * 传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
     *
     * @param conn
     *            传入数据库连接
     * @param sqlFile
     *            SQL 脚本文件    可选参数，为空字符串或为null时 默认路径为 src/test/resources/config/script.sql
     * @throws Exception
     */
    public static void execute(Connection conn, String sqlFile) throws Exception {
        PreparedStatement stmt;

        if(sqlFile==null||"".equals(sqlFile)){
            sqlFile="src/test/resources/config/script.sql";
        }
        List<String> sqlList = loadSql(sqlFile);
        stmt = conn.prepareStatement("");
        for (String sql : sqlList) {
            stmt.addBatch(sql);
        }
        int[] rows = stmt.executeBatch();
        System.out.println("Row count:" + Arrays.toString(rows));
        conn.commit();
        stmt.close();
        conn.close();
    }


    /**
     *  传入连接来执行 SQL 脚本文件，这样可与其外的数据库操作同处一个事物中
     * @param conn Connection对象
     * @param sqlList 全部sql执行语句
     * @param threadCount 第几个thread，从0开始
     * @throws Exception
     */
    public static void execute(Connection conn, List<String> sqlList, int threadCount) throws Exception {
        PreparedStatement stmt;
        stmt = conn.prepareStatement("");
        //每个线程需要处理多少
        int per = (int) Math.ceil((double) sqlList.size()/total_thread_num);
        //一共需要提交多少次事务
        int index = (int)Math.ceil((double)per/per_commit_num);
        for (int j = 0 ; j < index ; ++j){
            if (j < index-1){
                for (int i =threadCount*per+j*per_commit_num; i<threadCount*per+(j+1)*per_commit_num ;++i){
                    stmt.addBatch(sqlList.get(i));
                }
            }else {
                if (threadCount < total_thread_num-1){
                    for (int i = threadCount*per+j*per_commit_num; i < (threadCount+1)*per ;++i){
                        stmt.addBatch(sqlList.get(i));
                    }
                }else {
                    for (int i = threadCount*per+j*per_commit_num; i < sqlList.size() ;++i){
                        stmt.addBatch(sqlList.get(i));
                    }
                }
            }
            int[] rows = stmt.executeBatch();
            conn.commit();
            System.out.println("Row count:" + Arrays.toString(rows));
        }
        stmt.close();
        conn.close();
    }

    /**
     *  传入连接来执行 SQL 语句List，这样可与其外的数据库操作同处一个事物中
     * @param conn Connection对象
     * @param sqlList 全部sql执行语句
     * @throws Exception
     */
    public static void execute(Connection conn, List<String> sqlList) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement("");
        for (String sql : sqlList) {
            stmt.addBatch(sql);
        }
        int[] rows = stmt.executeBatch();
        conn.commit();
        System.out.println("Row count:" + Arrays.toString(rows));
        stmt.close();
        conn.close();
    }

    /**
     * 自动连接到数据库，返回Connection。由于需要这里面关闭了自动提交事务
     * @return
     */
    public static Connection Connect2Db(){
        String url = "jdbc:mysql://127.0.0.1:3306/MultithreadingTest";
        String name = "com.mysql.jdbc.Driver";
        String user = "root";
        String password = "";

        Connection conn = null;
        try {
            //动态加载驱动
            Class.forName(name);
            //获取连接
            conn = DriverManager.getConnection(url,user,password);
            //关闭自动提交
            conn.setAutoCommit(false);

            return conn;
        }catch (Exception e){
            e.printStackTrace();
            return null;
        }
    }
}


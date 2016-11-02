import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Date;

/**
 * Created by antdlx on 2016/11/1.
 * 一个最开始用于熟悉sql操作、多线程和文件读取的例子
 */
public class MyThreadTest extends Thread{
    public void run(){
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

        }catch (Exception e){
            e.printStackTrace();
        }

        //开始时间
        Long beginTime = new Date().getTime();
        //sql前缀
        String prefix = "INSERT INTO test_table(t_name,t_sex) VALUES ";
        try {
            //sql后缀
            StringBuffer suffix = new StringBuffer();
            conn.setAutoCommit(false);
            PreparedStatement pst = conn.prepareStatement("");
            // 外层循环，总提交事务次数
            for (int i = 1; i <=10 ; i++){
                suffix = new StringBuffer();
                for (int j = 1 ; j<=100; j++){
                    suffix.append("('antdlx','male'),");
                }
                // 构建完整SQL
                String sql = prefix + suffix.substring(0, suffix.length() - 1);
                // 添加执行SQL
                pst.addBatch(sql);
                // 执行操作
                pst.executeBatch();
                // 提交事务
                conn.commit();
                // 清空上一次添加的数据
                suffix = new StringBuffer();
            }
//            pst.addBatch("INSERT INTO `test_table` VALUES ('antdlx', 'male', '103201');");
            // 执行操作
            pst.executeBatch();
            // 提交事务
            conn.commit();
            // 关闭连接
            pst.close();
            conn.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        // 结束时间
        Long endTime = new Date().getTime();
        // 耗时
        System.out.println("1W条数据插入花费时间 : " + (endTime - beginTime) / 1000 + " s"+"  插入完成");
    }
}

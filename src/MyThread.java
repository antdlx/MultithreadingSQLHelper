import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by antdlx on 2016/11/1.
 * 用于多线程执行sql操作的线程类
 */
public class MyThread extends Thread{

    List<String> sqlList = new ArrayList<>();
    int threadCount = 0;

    public MyThread(List<String> sqlList,int threadCount){
        this.sqlList = sqlList;
        this.threadCount = threadCount;
    }

    public void run(){

        Connection conn = SQLTools.Connect2Db();

        try {
            conn.setAutoCommit(false);
            SQLTools.execute(conn,sqlList,threadCount);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}

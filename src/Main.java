import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by antdlx on 2016/11/1.
 */

public class Main {
    public static void main(String [] args){

        List<String> sqlList;
        List<String> headList = new ArrayList<>();

        try {
            sqlList = SQLTools.loadSql("C:\\Users\\antdlx\\Desktop\\test_table.sql");


            for (int i = 0 ; i < SQLTools.head_sql_num ; ++i){
                headList.add(sqlList.get(0));
                sqlList.remove(0);
            }
            Connection conn = SQLTools.Connect2Db();
            SQLTools.execute(conn,headList);

            for (int i = 0 ; i < SQLTools.total_thread_num ; i++){
                new MyThread(sqlList,i).start();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
//        new MyThreadTest().start();
    }

}

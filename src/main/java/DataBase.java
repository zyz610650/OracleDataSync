import java.sql.*;

public class DataBase {
//    private static String driver = "oracle.jdbc.driver.OracleDriver";
//
//    private String url = "jdbc:oracle:thin:@localhost:1521:orcl";
//
//    private String user = "XXX";//oracle数据库的用户名
//    private String pwd = "XXXXXX";//oracle数据库的用户密码
    private static PreparedStatement sta = null;
    private static ResultSet rs = null;
    private static Connection conn = null;

//    public static String DATABASE_DRIVER="oracle.jdbc.driver.OracleDriver";
//    public static String
//            SOURCE_DATABASE_URL="jdbc:oracle:thin:@127.0.0.1:1521:practice/orcl";

    /**
     * 加载驱动程序
     */
    static {
        try {
            Class.forName(Constants.DATABASE_DRIVER);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取源数据库连接
     */

    public static Connection getSourceDataBase()
    {
       Connection conn=getConn(Constants.SOURCE_DATABASE_URL,Constants.SOURCE_DATABASE_USERNAME,Constants.SOURCE_DATABASE_PASSWORD);
        System.out.println(conn);
       return conn;
    }

    /**
     * 获取目标数据库连接
     */
    public static Connection getTargetDataBase()
    {
        Connection conn= getConn(Constants.SOURCE_TARGET_URL,Constants.SOURCE_TARGET_USERNAME,Constants.SOURCE_TARGET_PASSWORD);
        System.out.println(conn);
        return conn;

    }
    /**
     * @return 连接对象
     */
    public static Connection getConn(String url, String user, String pwd) {
        try {
            conn = DriverManager.getConnection(url, user, pwd);
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return conn;
    }

    public static void main(String[] args) {
        getSourceDataBase();
    }

//    /**
//     * @param sql
//     *            sql语句  增加，删除，修改
//     * @param obj
//     *            参数
//     * @return
//     */
//    public int update(String sql, Object... obj) {
//        int count = 0;
//        conn = getConn();
//        try {
//            sta = conn.prepareStatement(sql);
//            if (obj != null) {
//                for (int i = 0; i < obj.length; i++) {
//                    sta.setObject(i + 1, obj[i]);
//                }
//            }
//            count = sta.executeUpdate();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally{
//
//            close();
//        }
//        return count;
//    }
//
//    /**
//     * @param sql sql语句
//     * @param obj 参数
//     * @return 数据集合
//     */
//    public ResultSet Query(String sql,Object...obj){
//        conn=getConn();
//        try {
//            sta=conn.prepareStatement(sql);
//            if(obj!=null){
//                for(int i=0;i<obj.length;i++){
//                    sta.setObject(i+1, obj[i]);
//                }
//            }
//            rs=sta.executeQuery();
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return rs;
//    }
//
//    /**
//     * 关闭资源
//     */
//    public void close() {
//        try {
//            if (rs != null) {
//                rs.close();
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        } finally {
//            try {
//                if (sta != null) {
//                    sta.close();
//                }
//            } catch (SQLException e2) {
//                e2.printStackTrace();
//            } finally {
//                if (conn != null) {
//                    try {
//                        conn.close();
//                    } catch (SQLException e) {
//                        e.printStackTrace();
//                    }
//                }
//            }
//        }
//    }

}

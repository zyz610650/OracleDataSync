public class Constants {
    /** 上次数据同步最后SCN号 */
    public static String LAST_SCN = "0";
    /** 源数据库配置 */
    public static String DATABASE_DRIVER="oracle.jdbc.driver.OracleDriver";
    public static String
    SOURCE_DATABASE_URL="jdbc:oracle:thin:@127.0.0.1:1521:zyz";
    public static String SOURCE_DATABASE_USERNAME="LOGMINER";
    public static String SOURCE_DATABASE_PASSWORD="LOGMINER";
    public static String SOURCE_CLIENT_USERNAME = "LOGMINER";
    /** 目标数据库配置 */
    public static String SOURCE_TARGET_URL="jdbc:oracle:thin:@127.0.0.1:1521:zyz";
    public static String SOURCE_TARGET_USERNAME="zyz";
    public static String SOURCE_TARGET_PASSWORD="zyz610650";
    /** 日志文件路径 */
    public static String LOG_PATH = "E:\\program\\oracleDataBase\\oradata\\zyz";
    /** 数据字典路径 */
    public static String DATA_DICTIONARY = "E:\\program\\oracleDataBase\\oradata\\practice\\LOGMNR";
}

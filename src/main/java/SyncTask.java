import java.sql.*;

public class SyncTask {

    private  static  String lastScn = Constants.LAST_SCN;
    private  static Connection sourceConn = null;
    private  static Connection targetConn = null;
    private  static ResultSet resultSet = null;
    private  static CallableStatement callableStatement=null;
    private  static Statement statement=null;
    /**
     * <p>方法名称: createDictionary|描述: 调用logminer
     /**
     * <p>方法名称: startLogmur|描述:启动
     生成数据字典文件</p>
     * @param sourceConn 源数据库连接
     * @throws Exception 异常信息
     */

    public static void createDictionary(Connection sourceConn) throws Exception{
        String createDictSql = "BEGIN dbms_logmnr_d.build(dictionary_filename => 'dictionary.ora', dictionary_location =>'"+Constants.DATA_DICTIONARY+"'); END;";
        CallableStatement callableStatement = sourceConn.prepareCall(createDictSql);
        callableStatement.execute();
    }


    /**
     * 启动Logminer分析
     * @throws Exception
     */
    public static void startLogmur() throws Exception{

        try {

// 获取源数据库连接
            sourceConn = DataBase.getSourceDataBase();
             statement = sourceConn.createStatement();
// 添加所有日志文件，本代码仅分析联机日志
            StringBuffer sbSQL = new StringBuffer();
            sbSQL.append(" BEGIN");
            sbSQL.append(" dbms_logmnr.add_logfile(logfilename=>'"+Constants.LOG_PATH+"\\REDO01.LOG', options=>dbms_logmnr.NEW);");
            sbSQL.append(" dbms_logmnr.add_logfile(logfilename=>'"+Constants.LOG_PATH+"\\REDO02.LOG', options=>dbms_logmnr.ADDFILE);");
            sbSQL.append(" dbms_logmnr.add_logfile(logfilename=>'"+Constants.LOG_PATH+"\\REDO03.LOG', options=>dbms_logmnr.ADDFILE);");
            sbSQL.append(" END;");
             callableStatement = sourceConn.prepareCall(sbSQL+"");
            callableStatement.execute();
// 打印获分析日志文件信息 v$logmnr_logs 分析日志列表视图
            resultSet = statement.executeQuery("SELECT db_name, thread_sqn, filename FROM v$logmnr_logs");
            while(resultSet.next()){
                System.out.println("已添加日志文件==>"+resultSet.getObject(3));
            }
            //日志分析
          while(true){
              analy();
              Thread.sleep(5000);
          }
        }
        finally{
            if( null != sourceConn ){
                sourceConn.close();
            }
            if( null != targetConn ){
                targetConn.close();
            }
            sourceConn = null;
            targetConn = null;
        }
    }

    public static void analy() throws Exception {

//            System.out.println("开始分析日志文件,起始scn号:"+Constants.LAST_SCN);
        //  callableStatement = sourceConn.prepareCall("BEGIN dbms_logmnr.start_logmnr(startScn=>'"+Constants.LAST_SCN+"',dictfilename=>'"+Constants.DATA_DICTIONARY+"\\dictionary.ora',OPTIONS =>DBMS_LOGMNR.COMMITTED_DATA_ONLY+dbms_logmnr.NO_ROWID_IN_STMT);END;");
        System.out.println("开始分析日志文件,起始scn号:"+lastScn);
        callableStatement = sourceConn.prepareCall("BEGIN dbms_logmnr.start_logmnr(startScn=>'"+lastScn+"',dictfilename=>'"+Constants.DATA_DICTIONARY+"\\dictionary.ora',OPTIONS =>DBMS_LOGMNR.COMMITTED_DATA_ONLY+dbms_logmnr.NO_ROWID_IN_STMT);END;");
        callableStatement.execute();
        System.out.println("完成分析日志文件");
// 查询获取分析结果
        System.out.println("查询分析结果");
        // sql_redo是解析处的sql
        resultSet = statement.executeQuery("SELECT scn,operation,timestamp,status,sql_redo FROM v$logmnr_contents WHERE seg_owner='"+Constants.SOURCE_CLIENT_USERNAME+"' AND seg_type_name='TABLE' AND operation !='SELECT_FOR_UPDATE'");

        // 连接到目标数据库，在目标数据库执行redo语句
        targetConn = DataBase.getTargetDataBase();
        Statement targetStatement = targetConn.createStatement();
//            String lastScn = Constants.LAST_SCN;


        String operation = null;
        String sql = null;
        boolean isCreateDictionary = false;
        while(resultSet.next()){
            lastScn = resultSet.getObject(1)+"";
            if( lastScn.equals(Constants.LAST_SCN) ){
                continue; }
            operation = resultSet.getObject(2)+"";
            if( "DDL".equalsIgnoreCase(operation) ){
                isCreateDictionary = true; }
            sql = resultSet.getObject(5)+"";
// 替换用户
            sql = sql.replace("\""+Constants.SOURCE_CLIENT_USERNAME+"\".", "");
            System.out.println("scn="+lastScn+",自动执行sql=="+sql+"");
            try {
                targetStatement.executeUpdate(sql.substring(0, sql.length()-1));
            } catch (Exception e) {
                System.out.println("scn:"+lastScn+" 语句已被执行,跳过");
            } }
// 更新scn
        Constants.LAST_SCN = (Integer.parseInt(lastScn))+"";
// DDL发生变化，更新数据字典
        if( isCreateDictionary ){
            System.out.println("DDL发生变化，更新数据字典");
            createDictionary(sourceConn);
            System.out.println("完成更新数据字典");
            isCreateDictionary = false; }
        System.out.println("完成一个工作单元,等待下一次解析\n\n");


    }
}




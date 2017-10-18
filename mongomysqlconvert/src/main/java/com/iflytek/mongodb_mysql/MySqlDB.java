package mongodb.iflytek.com;

import org.json.JSONObject;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

public class MySqlDB implements DataBase {

    private final static String SERVER="host";
    private final static String PORT = "port";
    private final static String DATABASE="database";
    private final static String USERNAME = "username";
    private final static String PASSWORD = "password";

    private  Connection conn =null;
    private JSONObject jsonConf;
    static private boolean loaded = false;
   static {
       try{
           Class.forName("com.mysql.jdbc.Driver");
           loaded =true;
       } catch (ClassNotFoundException e) {
           e.printStackTrace();
       }

   }
    public MySqlDB(JSONObject json) throws ClassNotFoundException {
        this.jsonConf = json;
        init();
    }
    private void init() throws ClassNotFoundException {
        if(!loaded)
            throw new ClassNotFoundException("mysql Driver loaded failed");
        try {
            conn = DriverManager.getConnection( this.jsonConf.getString(SERVER)+":"+this.jsonConf.getString(PORT)+"/"+this.jsonConf.getString(DATABASE),
                    this.jsonConf.getString(USERNAME)==null?"":this.jsonConf.getString(USERNAME),
                    this.jsonConf.getString(PASSWORD)==null?"":this.jsonConf.getString(PASSWORD)) ;
        }catch (SQLException e) {
            e.printStackTrace();
        }
    }


    public  Connection getConnecttion(){
        return conn;
    }
}

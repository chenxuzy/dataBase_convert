package com.iflytek.mongodb_mysql;

import com.mongodb.BasicDBObject;

import java.util.List;

public class KuyinUser {

    public static String mongodbCollectiopn;
    public static String mysqlTables;
    public static List<String> mysql_clomuns;
    public static List<String> mongo_clomuns;
    public static String insertSql;
    public static BasicDBObject query = new BasicDBObject();

    public static void SetInsertSql() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("replace into ");
        buffer.append(mysqlTables);
        buffer.append(" (");
        int index = 0;
        for (String key : mysql_clomuns) {
            if (index > 0)
                buffer.append(",");
            buffer.append("`");
            buffer.append(key);
            buffer.append("`");
            index++;
        }
        buffer.append(") values (");
        for (int i = 0, size = mysql_clomuns.size(); i < size; i++) {
            if (i > 0)
                buffer.append(",");
            buffer.append("?");
        }
        buffer.append(")");
        insertSql = buffer.toString();
        System.out.println(insertSql);
    }
}

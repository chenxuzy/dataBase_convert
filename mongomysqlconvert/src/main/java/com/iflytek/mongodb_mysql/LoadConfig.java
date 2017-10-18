package com.iflytek.mongodb_mysql;

import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;

public class LoadConfig {
    private static final String PER_FIX = "/config/";
    private static final String REAR = "-config.properties";
    private static final String REAR_JSON = "-config.json";

    public static Map<String,String> getConfig(String key){
    String filepath = PER_FIX+key+REAR;
        return loadConfig(filepath);
    }

    public static JSONObject getConfigFromJson(String fileName)  {
        String filepath = PER_FIX+fileName+REAR_JSON;
      try {
          JSONObject json = new JSONObject(load(filepath));
          return json;
      }catch (IOException e){
        return null;
      }catch (JSONException e){
          return null;
      }catch (Exception e){
          return  null;
      }

    }

    private static String load(String fileName)throws IOException{
        InputStream in =  LoadConfig.class.getResourceAsStream(fileName);
        StringBuilder buffer = new StringBuilder();
        BufferedReader reader = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while ((line = reader.readLine())!=null){
            buffer.append(line);
        }
        return buffer.toString();
    }

    private  static Map<String,String> loadConfig(String fileanme){

        try{
           Map<String ,String> config = new HashMap<>();
            Properties prop = new Properties();
            prop.load(LoadConfig.class.getResourceAsStream(fileanme));     ///加载属性列表
            Iterator<String> it=prop.stringPropertyNames().iterator();
            while(it.hasNext()){
                String key=it.next();
                config.put(key,prop.getProperty(key));
            }
            return config;
        }
        catch(Exception e){
            System.out.println(e);
            return null;
        }
    }
}

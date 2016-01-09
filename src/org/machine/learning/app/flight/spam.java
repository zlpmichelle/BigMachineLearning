package org.machine.learning.app.flight; /**
 * Created by Michelle on 1/17/15.
 */

import com.oracle.javafx.jmx.json.JSONException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.text.*;

import  net.sf.json.JSONArray;
import  net.sf.json.JSONObject;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

public class spam {

    public static void main(String[] args) {

        // read file content from file
        try {
            StringBuffer sb = new StringBuffer("");

            FileReader reader = new FileReader("/Users/Michelle/Desktop/CCP/P2/famous/web.log");
            BufferedReader br = new BufferedReader(reader);

            String str = null;
            ArrayList<HashMap> maps = new ArrayList<HashMap>();

            while ((str = br.readLine()) != null) {
                maps.add(parseJson(str));
            }

            System.out.println("finished all the json map objects construction");

            Arrays.sort(maps.toArray(), new UidComparator());
            System.out.println("sorted map objects by uid");

            String preUid = maps.get(0).get("uid").toString();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            Date  preDate = df.parse(maps.get(0).get("tstamp").toString());
            long  preDateTime= preDate.getTime();
            String preAction = maps.get(0).get("action").toString();

            for(int i = 1; i < maps.size(); i ++){
                String currentUid = maps.get(i).get("uid").toString();
                String campaign;
                String query;
                if(maps.contains("campaign")) {
                    campaign = maps.get(i).get("campaign").toString();
                }
                Date currentDate = df.parse(maps.get(i).get("tstamp").toString());
                long currentDateTime = currentDate.getTime();
                String experiments = maps.get(i).get("experiments").toString();
                String currentAction = maps.get(i).get("action").toString();
                if(maps.contains("query")) {
                    query = maps.get(i).get("query").toString();
                }

                if(currentUid.equals(preUid)){
                    if(!(preAction.equals("adclick") && currentAction.equals("landed"))) {
                        if (currentDateTime - preDateTime < 10000) {
                            sb.append(str + "/n");
                        }
                    }
                }

                preUid = maps.get(i).get("uid").toString();
                preDate = df.parse(maps.get(i).get("tstamp").toString());
                preDateTime = preDate.getTime();
                preAction = maps.get(i).get("action").toString();
            }

            System.out.println("filter bot");

            br.close();
            reader.close();


            // write string to file
            FileWriter writer = new FileWriter("/Users/Michelle/Desktop/CCP/P2/result/test2.txt");
            BufferedWriter bw = new BufferedWriter(writer);
            bw.write(sb.toString());

            bw.close();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }catch(Exception e){
            e.printStackTrace();
        }
    }


    //construct json from String and resolve it.
    public static HashMap parseJson(String line) {
        HashMap map = new HashMap();
        try {
            JSONObject jo = JSONObject.fromObject(line);
            map.put("visit_id", jo.getString("visit_id"));
            map.put("uid", jo.getString("uid"));
            if(jo.containsKey("campaign")) {
                map.put("campaign", jo.getString("campaign"));
            }
            map.put("tstamp", jo.getString("tstamp"));
            map.put("experiments", jo.getString("experiments"));
            map.put("action", jo.getString("action"));
            if(jo.containsKey("query")) {
                map.put("query", jo.getString("query"));
            }
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return map;
    }

}

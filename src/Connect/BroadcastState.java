package Connect;

import java.sql.DriverManager;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.json.JSONException;
import org.json.JSONObject;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;

import Server.HttpRequest;

public class BroadcastState {
    final static String ROOMINFOSERVERURL = "http://www.panda.tv/api_room";
    public static int getState(String roomID,int getStateCount,int startCount) throws JSONException {
        String requestData = HttpRequest.sendGet(ROOMINFOSERVERURL, "roomid=" + roomID);
        JSONObject json = new JSONObject(requestData);
        if (json.getJSONObject("data").getJSONObject("videoinfo").getString("status").equals("2")) {
            System.out.println(roomID + "is broadcasting");
            getStateCount++;
            //第一次获取到在直播时，存下直播开始时间
            if(getStateCount == 1){
                String driver = "com.mysql.jdbc.Driver";
                String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
                String user = "root";
                String password = "qwerty123456";
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                try {
                    Class.forName(driver);
                    Connection conn = (Connection) DriverManager.getConnection(url, user, password);
                    if (!conn.isClosed())
                        System.out.println("Succeeded connecting to the Database!");
                    String sql = "insert into broadcaststart (roomid, broadcastdate, starttime,startcount) values(?,?,?,?)";
                    PreparedStatement preparedStatement = (PreparedStatement) conn.prepareStatement(sql);
                    preparedStatement.setInt(1, Integer.parseInt(roomID));
                    preparedStatement.setString(2,sdf.format(new Date()));
                    preparedStatement.setTimestamp(3, new Timestamp(new Date().getTime()));
                    preparedStatement.setInt(4,startCount);
                    int re = preparedStatement.executeUpdate();
                    if (re > 0) {
                        System.out.println("Broadcasting Insert Start Time Success");
                    }
                    conn.close();
                } catch (Exception e) {
                    // TODO: handle exception
                    System.out.println("Sorry,can`t find the Driver!");
                    e.printStackTrace();
                }
            }
            return getStateCount;
        } else {
            System.out.println(roomID + "is not not broadcasting");
            return 0;
        }
    }

    public static int getVisitorNUm(String roomID) throws JSONException {
        String requestData = HttpRequest.sendGet(ROOMINFOSERVERURL, "roomid=" + roomID);
        JSONObject json = new JSONObject(requestData);
        String adNum = json.getJSONObject("data").getJSONObject("roominfo").getString("person_num");
        return Integer.parseInt(adNum);
    }
    public static int getFansNum(String roomID) throws JSONException {
        String requestData = HttpRequest.sendGet(ROOMINFOSERVERURL, "roomid=" + roomID);
        JSONObject json = new JSONObject(requestData);
        String fansNum = json.getJSONObject("data").getJSONObject("roominfo").getString("fans");
        return Integer.parseInt(fansNum);
    }

    public static int getBambooSum(String roomID) throws JSONException {
        String requestData = HttpRequest.sendGet(ROOMINFOSERVERURL, "roomid=" + roomID);
        JSONObject json = new JSONObject(requestData);
        String BambooSum = json.getJSONObject("data").getJSONObject("hostinfo").getString("bamboos");
        return Integer.parseInt(BambooSum);
    }

    public static String getClassification(String roomID) throws JSONException {
        String requestData = HttpRequest.sendGet(ROOMINFOSERVERURL, "roomid=" + roomID);
        JSONObject json = new JSONObject(requestData);
        String classification = json.getJSONObject("data").getJSONObject("roominfo").getString("classification");
        return classification;
    }
}

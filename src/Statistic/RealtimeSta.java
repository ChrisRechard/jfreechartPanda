package Statistic;

import Connect.BroadcastState;
import MainTest.GlobalVirables;

import com.mysql.jdbc.Statement;
import org.json.JSONException;

import java.sql.*;
import java.sql.Connection;
import java.sql.PreparedStatement;

import java.util.Date;

/**
 * 直播数据统计类，包含实时弹幕、竹子、猫币、人气、关注人数、竹子总数统计
 */
public class RealtimeSta extends GlobalVirables {

    /**
     * 弹幕数量实时统计方法
     * @param roomID 直播间号
     * @param startTimeCounts 当天开始直播次数计数（决定统计时间段开始时间）
     * @param danmuStatisticCount 弹幕统计次数计数（决定统计时段）
     */
    public static void danmuSta(String roomID,int startTimeCounts,int danmuStatisticCount){
        System.out.println("Enter statistic");
        System.out.println(startTimeCounts);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        int sum = 0;
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            Statement statement = (Statement) conn.createStatement();
            ResultSet rsStarttime = statement.executeQuery("select starttime from broadcaststart where"+" startcount = "+Integer.toString(startTimeCounts)+" AND"+" roomid = "+"\""+roomID+"\""+" AND broadcastdate = curdate()");
            rsStarttime.next();
            Timestamp bstarttime = rsStarttime.getTimestamp(1);
            //HH是二十四小时制 hh是十二小时制
            java.text.SimpleDateFormat forma = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //统计时间段开始时间和结束时间获取
            String start = forma.format(bstarttime.getTime()+(long)(danmuStatisticCount-1)*2*60*1000);
            //System.out.println(start);
            //统计3分钟内的弹幕数量 并存入数据库
            String end = forma.format(bstarttime.getTime()+danmuStatisticCount*2*60*1000);
            //System.out.println(end);
            ResultSet rs = statement.executeQuery("select count(*) from danmuinfo where "+"roomid = "+"\""+roomID+"\""+" AND recTime >= "+"\""+start+"\""+"AND recTime <= "+"\""+end+"\"");
            rs.next();
            sum = rs.getInt(1);

            String insertdata = "insert into danmuSta (roomid, periodstart, danmunum) values(?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(insertdata);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(bstarttime.getTime()+(long)(danmuStatisticCount)*2*60*1000));
            preparedStatement.setInt(3, sum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert DanmuStatistic Success rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr");
            }
            System.out.println(sum);
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    /**
     * 收到的竹子实时统计方法
     * @param roomID 统计的直播间ID
     * @param startTimeCounts 该直播间当天开始直播次数计数（决定统计时段的开始时间）
     * @param BambooRecStatisticCount 竹子统计次数计数（决定统计时段）
     */
    public static void BambooRecSta(String roomID,int startTimeCounts,int BambooRecStatisticCount){
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        int BambooSum = 0;
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            Statement statement = (Statement) conn.createStatement();
            ResultSet rsStarttime = statement.executeQuery("select starttime from broadcaststart where"+" startcount = "+Integer.toString(startTimeCounts)+" AND"+" roomid = "+"\""+roomID+"\""+" AND broadcastdate = curdate()");
            rsStarttime.next();
            Timestamp bstarttime = rsStarttime.getTimestamp(1);
            //HH是二十四小时制 hh是十二小时制
            java.text.SimpleDateFormat forma = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //统计时间段开始时间和结束时间获取
            String start = forma.format(bstarttime.getTime()+(long)(BambooRecStatisticCount-1)*3*60*1000);
            //System.out.println(start);
            //统计3分钟内的收到的竹子数量 并存入数据库
            String end = forma.format(bstarttime.getTime()+BambooRecStatisticCount*3*60*1000);
            ResultSet rs = statement.executeQuery("select zhuzi from zhuziinfo where "+"roomid = "+"\""+roomID+"\""+" AND recTime >= "+"\""+start+"\""+"AND recTime <= "+"\""+end+"\"");
            while (rs.next()){
                BambooSum+=rs.getInt("zhuzi");
            }
            String insertdata = "insert into bambooSta (roomid, start, bamboorec) values(?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(insertdata);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(bstarttime.getTime()+(long)(BambooRecStatisticCount-1)*3*60*1000));
            preparedStatement.setInt(3,BambooSum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert BambooStatistic Success rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr");
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 收到的礼物兑换成猫币实时统计
     * @param roomID 统计的直播间ID
     * @param startTimeCounts 该直播间当天开始直播次数计数（决定统计时段的开始时间）
     * @param PresentRecStatisticCount 礼物统计次数计数（决定统计时段）
     */
    public static void PresentRecSta(String roomID,int startTimeCounts,int PresentRecStatisticCount){
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        int maobiSum = 0;
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            Statement statement = (Statement) conn.createStatement();
            ResultSet rsStarttime = statement.executeQuery("select starttime from broadcaststart where"+" startcount = "+Integer.toString(startTimeCounts)+" AND"+" roomid = "+"\""+roomID+"\""+" AND broadcastdate = curdate()");
            rsStarttime.next();
            Timestamp bstarttime = rsStarttime.getTimestamp(1);
            //HH是二十四小时制 hh是十二小时制
            java.text.SimpleDateFormat forma = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            //统计时间段开始时间和结束时间获取
            String start = forma.format(bstarttime.getTime()+(long)(PresentRecStatisticCount-1)*3*60*1000);
            //System.out.println(start);
            //统计3分钟内的猫币数量 并存入数据库
            String end = forma.format(bstarttime.getTime()+PresentRecStatisticCount*3*60*1000);
            ResultSet rs = statement.executeQuery("select presentvalue from presentinfo where "+"roomid = "+"\""+roomID+"\""+" AND recTime >= "+"\""+start+"\""+"AND recTime <= "+"\""+end+"\"");
            while (rs.next()){
                maobiSum+=rs.getInt("presentvalue");
            }
            String insertdata = "insert into maobiSta (roomid, start, maobirec) values(?,?,?)";
            PreparedStatement preparedStatement = conn.prepareStatement(insertdata);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(bstarttime.getTime()+(long)(PresentRecStatisticCount-1)*3*60*1000));
            preparedStatement.setInt(3,maobiSum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert MaobiStatistic Success rrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrrr");
            }
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 直播间人气实时统计
     * @param roomID 统计的直播间ID
     * @throws JSONException
     */
    public static void VisitorSta(String roomID) throws JSONException {
        int adiNum = BroadcastState.getVisitorNUm(roomID);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        try {
            Class.forName(driver);
            com.mysql.jdbc.Connection conn = (com.mysql.jdbc.Connection) DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            String sql = "insert into vistorSta (roomid, recTime, audienceNum) values(?,?,?)";
            com.mysql.jdbc.PreparedStatement preparedStatement = (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(sql);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(new java.util.Date().getTime()));
            preparedStatement.setInt(3, adiNum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert Audience Number Success");
            }
            // conn.commit();
            conn.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }
    }

    /**
     * 直播间关注人数实时统计
     * @param roomID 统计的直播间ID
     * @throws JSONException
     */
    public static void FansNumSta(String roomID) throws JSONException {
        int fansNum = BroadcastState.getFansNum(roomID);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        try {
            Class.forName(driver);
            com.mysql.jdbc.Connection conn = (com.mysql.jdbc.Connection) DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            String sql = "insert into fansnumSta (roomid, curtime, curfansnum) values(?,?,?)";
            com.mysql.jdbc.PreparedStatement preparedStatement = (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(sql);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(new Date().getTime()));
            preparedStatement.setInt(3, fansNum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert FansSum Success");
            }
            // conn.commit();
            conn.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }
    }

    public static void BambooSumSta(String roomID) throws JSONException {
        int BambooSum = BroadcastState.getBambooSum(roomID);
        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        try {
            Class.forName(driver);
            com.mysql.jdbc.Connection conn = (com.mysql.jdbc.Connection) DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            String sql = "insert into bambooSumSta (roomid, curtime, bamboosum) values(?,?,?)";
            com.mysql.jdbc.PreparedStatement preparedStatement = (com.mysql.jdbc.PreparedStatement) conn.prepareStatement(sql);
            preparedStatement.setInt(1, Integer.parseInt(roomID));
            preparedStatement.setTimestamp(2, new Timestamp(new Date().getTime()));
            preparedStatement.setInt(3, BambooSum);
            int re = preparedStatement.executeUpdate();
            if (re > 0) {
                System.out.println("Insert BambooSum Success");
            }
            // conn.commit();
            conn.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }

    }


}

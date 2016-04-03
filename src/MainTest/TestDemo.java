package MainTest;

import Connect.BroadcastState;
import Connect.ConnectDanMuServer;

import Statistic.RealtimeSta;
import com.mysql.jdbc.Statement;
import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.StandardChartTheme;
import org.jfree.chart.axis.DateAxis;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYItemRenderer;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.renderer.xy.XYShapeRenderer;
import org.jfree.data.time.*;
import org.jfree.data.xy.XYDataset;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.jfree.ui.RefineryUtilities;
import org.json.JSONException;

import javax.swing.*;
import java.awt.*;
import java.sql.*;
import java.util.*;

import static org.jfree.chart.demo.TimeSeriesChartDemo1.createDemoPanel;


/**
 * 直播状态监听进程
 */
class broadcastListen extends GlobalVirables implements Runnable{

    private boolean isBroadcastStart = false;
    private int getCount = 0;
    private int getEndCount = 1;
    private int startTimesCount = 0;
    private String roomID;
    private volatile boolean isThreadStop = false;
    private fetchInfo proFetch;
    private DanmuStatistic DanmuThread;
    private BambooStatistic BambooRecThread;
    private MaobiStatistic MaobiThread;
    private VisitorStatistic VisitorThread;
    private FansNumStatistic FansSumThread;
    private BambooSumStatistic BambooSumThread;
    public broadcastListen(String rstring)
    {
        this.roomID=rstring;
    }
    //直播状态监听进程关闭方法
    public void closeThread(){
        getEndCount=0;
        DanmuThread.close();
        isThreadStop=true;
        proFetch.close();
        BambooRecThread.close();
        MaobiThread.close();
        VisitorThread.close();
        FansSumThread.close();
        BambooSumThread.close();
    }
    public void run(){

        while (!isThreadStop)
        {
            //每次抓取时 初始化getcount
            getCount=0;
            //直播开始次数计数
            startTimesCount++;
            while (!isBroadcastStart)
            {
                try {
                    //等待直播开始
                    //System.out.println(roomID);
                    getCount = BroadcastState.getState(roomID,getCount,startTimesCount);
                    if (getCount != 0){
                        isBroadcastStart = true;
                        //直播开始 开始抓取信息
                        ConnectDanMuServer fetch = new ConnectDanMuServer();
                        proFetch =new fetchInfo(roomID,fetch);
                        new Thread(proFetch).start();
                        //开始统计弹幕
                        DanmuThread =new DanmuStatistic(roomID,startTimesCount);
                        new Thread(DanmuThread).start();
                        //开始统计收到的竹子
                        BambooRecThread =new BambooStatistic(roomID,startTimesCount);
                        new Thread(BambooRecThread).start();
                        //开始统计收到的猫币
                        MaobiThread = new MaobiStatistic(roomID,startTimesCount);
                        new Thread(MaobiThread).start();
                        //开始获取实时人气
                        VisitorThread = new VisitorStatistic(roomID);
                        new Thread(VisitorThread).start();
                        //开始获取实时关注人数
                        FansSumThread = new FansNumStatistic(roomID);
                        new Thread(FansSumThread).start();
                        //开始获取实时竹子总数
                        BambooSumThread = new BambooSumStatistic(roomID);
                        new Thread(BambooSumThread).start();
                    }

                } catch (JSONException e) {
                    e.printStackTrace();
                }
                if (getCount ==0){
                    try {
                        Thread.currentThread().sleep((long)60000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

            }
            if(isBroadcastStart)
            {
                while (getEndCount !=0) {
                    try {
                        //直播开始后  等待直播结束
                        getEndCount = BroadcastState.getState(roomID, getEndCount,startTimesCount);
                    } catch (JSONException e) {
                        e.printStackTrace();
                    }
                    if (getEndCount != 0){
                        try {
                            Thread.currentThread().sleep((long)60000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }

                }
                if(getEndCount == 0)
                {
                    isBroadcastStart=false;
                    //直播结束 关闭信息抓取线程
                    proFetch.close();
                    //关闭弹幕统计线程
                    DanmuThread.close();
                    //关闭竹子统计线程
                    BambooRecThread.close();
                    //关闭猫币统计线程
                    MaobiThread.close();
                    //关闭人气获取线程
                    VisitorThread.close();
                    //关闭关注人数获取线程
                    FansSumThread.close();
                    //关闭竹子总数获取线程
                    BambooSumThread.close();
                }

            }
        }

    }

}

/**
 * 直播信息抓取线程
 */

class fetchInfo implements Runnable{
    private String roomID;
    private ConnectDanMuServer fetches;
    public fetchInfo(String roomstring,ConnectDanMuServer fet){
        this.roomID=roomstring;
        this.fetches=fet;
    }
    public void close(){

        this.fetches.Close();
    }
    public void run(){

        fetches.ConnectToDanMuServer(roomID);
    }
}

/**
 * 弹幕统计线程
 */
class DanmuStatistic implements Runnable{
    private String roomID;
    private int stacounts;
    private volatile boolean isDanmuStatisticStop=false;
    public void close(){

        isDanmuStatisticStop=true;
    }
    public DanmuStatistic(String roomString,int stacount){
        this.roomID=roomString;
        this.stacounts=stacount;
    }
    public void run(){
        //弹幕统计线程开始时统计弹幕初始化
        int danmustatistic =0;
        while(!isDanmuStatisticStop){
            danmustatistic++;
            try {
                Thread.currentThread().sleep((long)125*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.danmuSta(roomID,stacounts,danmustatistic);
        }
    }
}

/**
 * 实时收到的竹子统计线程
 */
class BambooStatistic implements Runnable{
    private String roomID;
    private int starcounts;
    private volatile boolean isBambooStatisticStop = false;
    public void close(){
        isBambooStatisticStop = true;
    }
    public BambooStatistic(String room,int starcount){
        this.roomID = room;
        this.starcounts = starcount;
    }
    public void run(){
        int bamboostatistic = 0;
        while (!isBambooStatisticStop){
            bamboostatistic++;
            try {
                Thread.currentThread().sleep((long)200*1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.BambooRecSta(roomID,starcounts,bamboostatistic);
        }
    }

}

/**
 * 实时收到的猫币统计线程
 */
class MaobiStatistic implements Runnable{
    private String roomID;
    private int starcounts;
    private volatile boolean isMaobiStatisticStop = false;
    public void close(){
        isMaobiStatisticStop = true;
    }
    public MaobiStatistic(String room,int starcount){
        this.roomID = room;
        this.starcounts = starcount;
    }
    public void run() {
        int maobistatistic = 0;
        while (!isMaobiStatisticStop) {
            maobistatistic++;
            try {
                Thread.currentThread().sleep((long) 200 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            RealtimeSta.PresentRecSta(roomID, starcounts, maobistatistic);
        }
    }
}
/**
 * 实时人气获取线程，每两分钟获取一次
 */
class VisitorStatistic implements Runnable{
    private String roomID;
    private volatile boolean isVisitorStatisticStop = false;
    public void close(){
        isVisitorStatisticStop = true;
    }
    public VisitorStatistic (String room){
        this.roomID = room;
    }
    public void run() {
        while (!isVisitorStatisticStop) {
            try {
                RealtimeSta.VisitorSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.currentThread().sleep((long) 120 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

/**
 * 直播关注人数实时获取，每两分钟获取一次
 */
class FansNumStatistic implements Runnable{
    private String roomID;
    private volatile boolean isFansNumStatisticStop = false;
    public void close(){
        isFansNumStatisticStop = true;
    }
    public FansNumStatistic (String room){
        this.roomID = room;
    }
    public void run() {
        while (!isFansNumStatisticStop) {
            try {
                RealtimeSta.FansNumSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.currentThread().sleep((long) 300 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

/**
 * 直播竹子总数获取，每两分钟获取一次
 */
class BambooSumStatistic implements Runnable{
    private String roomID;
    private volatile boolean isBambooSumStatisticStop = false;
    public void close(){
        isBambooSumStatisticStop = true;
    }
    public BambooSumStatistic (String room){
        this.roomID = room;
    }
    public void run() {
        while (!isBambooSumStatisticStop) {
            try {
                RealtimeSta.BambooSumSta(roomID);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            try {
                Thread.currentThread().sleep((long) 300 * 1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
public class TestDemo extends JFrame{

    public TestDemo(String s,String room) {
        super(s);
        setContentPane(createDemoPanel(room));
    }
    static TimeSeries xyseries;
    public XYDataset createDataset1(String room) {
        xyseries = new TimeSeries(room);
        TimeSeriesCollection xyseriescollection = new TimeSeriesCollection();
        xyseriescollection.addSeries(xyseries);
        return xyseriescollection;
    }
    public JFreeChart createChart(String roomID) {
        XYDataset xydataset = createDataset1(roomID);
        StandardChartTheme standardChartTheme=new StandardChartTheme("CN");
        //设置标题字体
        standardChartTheme.setExtraLargeFont(new Font("隶书",Font.BOLD,20));
        //设置图例的字体
        standardChartTheme.setRegularFont(new Font("宋书",Font.PLAIN,15));
        //设置轴向的字体
        standardChartTheme.setLargeFont(new Font("宋书",Font.PLAIN,15));
        //应用主题样式
        ChartFactory.setChartTheme(standardChartTheme);
        JFreeChart jfreechart = ChartFactory.createTimeSeriesChart(
                "弹幕数量检测图", "时间", "弹幕数量", xydataset,
                true,true, false);
        XYPlot xyplot = (XYPlot) jfreechart.getPlot();
        //NumberAxis numberaxis = (NumberAxis) xyplot.getRangeAxis();
        //numberaxis.setAutoRangeIncludesZero(true);
        DateAxis x = (DateAxis) xyplot.getDomainAxis();
        x.setAutoRange(true);
        NumberAxis y =(NumberAxis) xyplot.getRangeAxis();
        y.setAutoRange(true);
        x.setLabel("时间");
        x.setAutoRange(true);
        XYItemRenderer r = xyplot.getRenderer();
        if (r instanceof XYLineAndShapeRenderer) {
            XYLineAndShapeRenderer renderer = (XYLineAndShapeRenderer) r;
            renderer.setBaseShapesVisible(true);    // 数据点显示外框
            renderer.setBaseShapesFilled(true);
            renderer.setSeriesFillPaint(0, Color.ORANGE);
        }
        return jfreechart;
    }
    public  void dynamicRun(String roomID) {
        while (true) {
            try {
                Thread.currentThread();
                Thread.sleep(130000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            String driver = "com.mysql.jdbc.Driver";
            String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
            String user = "root";
            String password = "qwerty123456";
            try {
                Class.forName(driver);
                Connection conn = DriverManager.getConnection(url, user, password);
                if (!conn.isClosed())
                    System.out.println("Succeeded connecting to the Database!");
                Statement statement = (Statement) conn.createStatement();
                ResultSet getdanmusta = statement.executeQuery("select * from danmusta where"+" roomid = "+"\""+roomID+"\"");
                getdanmusta.last();
                Timestamp temp = getdanmusta.getTimestamp("periodstart");
                int danmunum = getdanmusta.getInt("danmunum");
//                java.text.SimpleDateFormat forma = new java.text.SimpleDateFormat("HH:mm:ss");
//                String tim = forma.format(temp.getTime());
                xyseries.add(new Second(new java.util.Date(temp.getTime())),danmunum);
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }

        }
    }
    public  JPanel createDemoPanel(String room) {
        JFreeChart jfreechart = createChart(room);
        return new ChartPanel(jfreechart);
    }
    public static void main(String[] args){

        String driver = "com.mysql.jdbc.Driver";
        String url = "jdbc:mysql://127.0.0.1:3306/panda?useSSL=true";
        String user = "root";
        String password = "qwerty123456";
        try {
            Class.forName(driver);
            Connection conn = DriverManager.getConnection(url, user, password);
            if (!conn.isClosed())
                System.out.println("Succeeded connecting to the Database!");
            Statement statement = (Statement) conn.createStatement();
            //存储弹幕信息的表
            //String sql2 = "create table IF NOT EXISTS danmuinfo (roomId integer,recTime TIMESTAMP,danmu text)";
            String sql1 = "create table IF NOT EXISTS danmuinfo (roomid integer,recTime TIMESTAMP)";
            String sql2 = "create table IF NOT EXISTS zhuziinfo (roomid integer,recTime TIMESTAMP,zhuzi integer)";
            String sql3 = "create table IF NOT EXISTS presentinfo (roomid integer,recTime TIMESTAMP,presentvalue INTEGER)";
            String sql4 = "create table IF NOT EXISTS broadcaststart (roomid integer,broadcastdate text,starttime TIMESTAMP,startcount INTEGER)";
            String sql5 = "create table IF NOT EXISTS vistorSta (roomId integer,recTime TIMESTAMP,audienceNum integer)";
            String sql6 = "create table IF NOT EXISTS danmuSta (roomId integer,periodstart TIMESTAMP,danmunum INTEGER)";
            String sql7 = "create table IF NOT EXISTS bambooSta (roomId integer,start TIMESTAMP,bamboorec INTEGER)";
            String sql8 = "create table IF NOT EXISTS maobiSta (roomId integer,start TIMESTAMP,maobirec INTEGER)";
            String sql9 = "create table IF NOT EXISTS fansnumSta (roomId integer,curtime TIMESTAMP,curfansnum INTEGER)";
            String sql10 = "create table IF NOT EXISTS bambooSumSta (roomId integer,curtime TIMESTAMP,bamboosum INTEGER)";
            int r1 = statement.executeUpdate(sql1);
            int r2 = statement.executeUpdate(sql2);
            int r3 = statement.executeUpdate(sql3);
            int r4 = statement.executeUpdate(sql4);
            int r5 = statement.executeUpdate(sql5);
            int r6 = statement.executeUpdate(sql6);
            int r7 = statement.executeUpdate(sql7);
            int r8 = statement.executeUpdate(sql8);
            int r9 = statement.executeUpdate(sql9);
            int r10 = statement.executeUpdate(sql10);
            if (r1 == -1) {
                System.out.println("Create danmuInfo Fails");
            }
            if (r2 == -1) {
                System.out.println("Create zhuziInfo Fails");
            }
            if (r3 == -1) {
                System.out.println("Create persentInfo Fails");
            }
            if (r4 == -1) {
                System.out.println("Create startInfo Fails");
            }
            if (r5 == -1) {
                System.out.println("Create VisitorSta Fails");
            }
            if (r6 == -1) {
                System.out.println("Create DanmuSta Fails");
            }
            if (r7 == -1) {
                System.out.println("Create BambooSta Fails");
            }
            if (r8 == -1) {
                System.out.println("Create maobiSta Fails");
            }
            if (r9 == -1) {
                System.out.println("Create fansnumSta Fails");
            }
            if (r10 == -1) {
                System.out.println("Create BambooSUM SUM SUM Sta Fails");
            }
            conn.close();
        } catch (Exception e) {
            // TODO: handle exception
            System.out.println("Sorry,can`t find the Driver!");
            e.printStackTrace();
        }
        String room1 = "8002";
        broadcastListen L1 = new broadcastListen(room1);
        new Thread(L1).start();

        JFrame.setDefaultLookAndFeelDecorated(true);
        TestDemo annotationdemo2 = new TestDemo("弹幕实时监控",room1);
        annotationdemo2.pack();
        annotationdemo2.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        RefineryUtilities.centerFrameOnScreen(annotationdemo2);
        annotationdemo2.setVisible(true);
        annotationdemo2.dynamicRun(room1);

    }
}

package com.etr.script;

import com.alibaba.fastjson2.JSON;
import com.etr.script.entity.InvoiceCostEntity;
import com.etr.script.entity.WalletNoAndUserIdEntity;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Init {


    private static final Logger logger = LoggerFactory.getLogger(Init.class);


    private static Connection conn = null;
    private static Statement stmt = null;

    private static RestHighLevelClient client;

    private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(5, 20, 7, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

    private static final Integer MAX_VALUE = 10000;


    public static void main(String[] args) {

        Properties properties = new Properties();
        try {
            // 加载配置文件
            properties.load(new FileInputStream("src\\main\\resources\\config.properties"));
            // 读取配置项
            String table = properties.getProperty("table");
            initConnect(properties);
            if ("user_account".equals(table)) {
                userAccount();
            }
            if ("wallet_account_record".equals(table)) {
                walletAccountRecord();
            }
            if ("pay_record".equals(table)) {
                payRecord();
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (stmt != null) {
                try {
                    stmt.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }

            logger.info("completion of task");
            System.exit(0);
        }

    }

    public static void initConnect(Properties properties) {
        logger.info("start init database connect");
        String url = properties.getProperty("url");
        String username = properties.getProperty("userName");
        String password = properties.getProperty("passWorld");

        String esUrl = properties.getProperty("esUrl");
        String esUserName = properties.getProperty("esUserName");
        String esPassWord = properties.getProperty("esPassWord");

        try {
            // 加载数据库驱动程序
            Class<?> clazz = Class.forName("com.mysql.cj.jdbc.Driver");
            // 建立数据库连接
            conn = DriverManager.getConnection(url, username, password);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }

        List<HttpHost> hostList = new ArrayList<>();
        Arrays.stream(esUrl.split(","))
                .forEach(item -> hostList.add(new HttpHost(item.split(":")[0],
                        Integer.parseInt(item.split(":")[1]), "http")));
        // 转换成 HttpHost 数组
        HttpHost[] httpHost = hostList.toArray(new HttpHost[]{});
        RestClientBuilder builder = RestClient.builder(httpHost);
        builder.setHttpClientConfigCallback(httpClientBuilder -> {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            // 设置账号密码
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(esUserName,
                            esPassWord));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
            return httpClientBuilder;
        });
        client = new RestHighLevelClient(builder);
        logger.info(" init database connect is finished");
    }


    public static void userAccount() {
        // 创建Statement对象
        String countSql = "SELECT count(*) as 'count' FROM user_account";
        String sql = "SELECT id,wallet_no FROM user_account";
        ResultSet rscount = null;
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("wallet_user_id");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wallet_user_id");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            stmt = conn.createStatement();

            rscount = stmt.executeQuery(countSql);
            while (rscount.next()) {
                int count = rscount.getInt("count");
                logger.info("user account count:" + rscount);

                int start = 0;
                int times = count / MAX_VALUE;
                for (int i = 0; i <= times; i++) {
                    start = MAX_VALUE * i;
                    int finalStart = start;

//                    EXECUTOR.execute(() -> {
                    String s = sql + "  limit " + finalStart + "," + MAX_VALUE;
                    ResultSet rs = null;
                    try {
                        Statement statement = conn.createStatement();
                        rs = statement.executeQuery(s);
                        BulkRequest bulkRequest = new BulkRequest();
                        while (rs.next()) {
                            String id = rs.getString("id");
                            String walletNo = rs.getString("wallet_no");
                            if (walletNo != null && !"".equals(walletNo)) {
                                WalletNoAndUserIdEntity entity = new WalletNoAndUserIdEntity();
                                entity.setUserId(id);
                                entity.setWalletNo(walletNo);
                                IndexRequest request = buildIndexRequest(entity, "wallet_user_id", id);
                                bulkRequest.add(request);
                            }

                        }
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (SQLException throwables) {
                        logger.error("error", throwables);
                    } catch (IOException e) {
                        logger.error("error", e);
                    } finally {
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (SQLException throwables) {
                                throwables.printStackTrace();
                            }
                        }
                    }
//                    });
                }

            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        } finally {
            if (rscount != null) {
                try {
                    rscount.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    public static void walletAccountRecord() {
        String countSql = "SELECT COUNT(*) AS 'COUNT' FROM" +
                "( SELECT t1.* FROM wallet_account_record t1  GROUP BY t1.pay_no HAVING SUM(CASE WHEN (operate_type=4 AND refund_status IN(0,1,2) ) THEN 1 ELSE 0 END )=0) t2   " +
                "WHERE  t2.pay_status=2 AND t2.operate_type=1 AND t2.operate_des=\"首次充值\" ";

        String sql = "SELECT t3.amount,t3.booked_time,t3.pay_no,t4.id FROM " +
                "( SELECT t2.amount,t2.booked_time,t2.pay_no,t2.wallet_no FROM" +
                "( SELECT t1.* FROM wallet_account_record t1  GROUP BY t1.pay_no HAVING SUM(CASE WHEN (operate_type=4 AND refund_status IN(0,1,2) ) THEN 1 ELSE 0 END )=0) t2 " +
                " \n" +
                " WHERE  t2.pay_status=2 AND t2.operate_type=1 AND t2.operate_des=\"首次充值\") t3 " +
                " JOIN user_account t4 ON t4.wallet_no=t3.wallet_no ";
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("invoice");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("invoice");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            stmt = conn.createStatement();
            ResultSet rscount = stmt.executeQuery(countSql);
            logger.info("wallet_account_record count:" + rscount);
            while (rscount.next()) {
                int count = rscount.getInt("count");
                int start = 0;
                int times = count / MAX_VALUE;
                for (int i = 0; i <= times; i++) {
                    start = MAX_VALUE * i;
                    int finalStart = start;
//                    EXECUTOR.execute(() -> {
                    String s = sql + "  LIMIT " + finalStart + "," + MAX_VALUE;
                    ResultSet rs = null;
                    try {
                        Statement statement = conn.createStatement();
                        rs = statement.executeQuery(s);
                        BulkRequest bulkRequest = new BulkRequest();
                        while (rs.next()) {
                            String userId = rs.getString("id");
                            String amount = rs.getString("amount");
                            String bookedTime = rs.getString("booked_time");
                            String payNo = rs.getString("pay_no");
                            if (payNo != null && !"".equals(payNo)) {
                                InvoiceCostEntity entity = new InvoiceCostEntity();
                                entity.setUserId(userId);
                                entity.setTradeNo(payNo);
                                entity.setPayTime(bookedTime);
                                entity.setCostType(1);
                                entity.setVersion("1");
                                entity.setCostCount(Long.valueOf(amount));
                                entity.setForm("wallet_account_record");
                                entity.setStatus(0);
                                IndexRequest request = buildIndexRequest(entity, "invoice", payNo);
                                bulkRequest.add(request);
                            }
                        }
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (SQLException throwables) {
                        logger.error("error", throwables);
                    } catch (IOException e) {
                        logger.error("error", e);
                    }
//                    });
                }

            }

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        }

    }

    public static void payRecord() {
        String countSql = "SELECT count(*) as 'count'  FROM pay_record WHERE pay_biz=4 AND `status`=2";
        String sql = "SELECT user_id,trade_no,fee_amount,pay_time FROM pay_record WHERE pay_biz=4 AND `status`=2";
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("invoice");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("invoice");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            stmt = conn.createStatement();
            ResultSet rscount = stmt.executeQuery(countSql);
            logger.info("payRecord count:" + rscount);
            while (rscount.next()) {
                int count = rscount.getInt("count");
                int start = 0;
                int times = count / MAX_VALUE;
                for (int i = 0; i <= times; i++) {
                    start = MAX_VALUE * i;
                    int finalStart = start;
//                    EXECUTOR.execute(() -> {
                    String s = sql + "  LIMIT " + finalStart + "," + MAX_VALUE;
                    ResultSet rs = null;
                    try {
                        Statement statement = conn.createStatement();
                        rs = statement.executeQuery(s);
                        BulkRequest bulkRequest = new BulkRequest();
                        while (rs.next()) {
                            String userId = rs.getString("user_id");
                            String tradeNo = rs.getString("trade_no");
                            String feeAmount = rs.getString("fee_amount");
                            String payTime = rs.getString("pay_time");
                            InvoiceCostEntity entity = new InvoiceCostEntity();
                            entity.setUserId(userId);
                            entity.setTradeNo(tradeNo);
                            entity.setPayTime(payTime);
                            entity.setCostType(1);
                            entity.setVersion("1");
                            entity.setCostCount(Long.valueOf(feeAmount));
                            entity.setForm("pay_record");
                            entity.setStatus(0);
                            IndexRequest request = buildIndexRequest(entity, "invoice", tradeNo);
                            bulkRequest.add(request);
                        }
                        client.bulk(bulkRequest, RequestOptions.DEFAULT);
                    } catch (SQLException throwables) {
                        logger.error("error", throwables);
                    } catch (IOException e) {
                        logger.error("error", e);
                    }
//                    });
                }

            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        }

    }


    private static <T> IndexRequest buildIndexRequest(T entity, String indexName, String id) {
        IndexRequest indexRequest = new IndexRequest();
        // 构建插入的json格式数据
        String json = JSON.toJSONString(entity);
        indexRequest.index(indexName);
        indexRequest.source(json, XContentType.JSON);
        indexRequest.id(id);
        return indexRequest;
    }
}



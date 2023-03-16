package com.etr.script;

import com.alibaba.fastjson2.JSON;
import com.etr.script.entity.WalletNoAndUserIdEntity;
import com.sun.org.slf4j.internal.Logger;
import com.sun.org.slf4j.internal.LoggerFactory;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class Init {

    private static final Logger LOGGER = LoggerFactory.getLogger(Init.class);

    private static Connection conn = null;
    private static Statement stmt = null;

    private static RestHighLevelClient client;

    private static final ThreadPoolExecutor EXECUTOR = new ThreadPoolExecutor(5, 20, 7, TimeUnit.SECONDS,
            new LinkedBlockingDeque<>(100), new ThreadPoolExecutor.CallerRunsPolicy());

    private static Integer MAX_VALUE = 10000;

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
        }

    }

    public static void initConnect(Properties properties) {
        LOGGER.debug("start init connect");
        String url = properties.getProperty("url");
        String username = properties.getProperty("userName");
        String password = properties.getProperty("passWorld");

        String esUrl = properties.getProperty("esUrl");
        String esUserName = properties.getProperty("esUserName");
        String esPassWord = properties.getProperty("esPassWord");

        try {
            // 加载数据库驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            // 建立数据库连接
            conn = DriverManager.getConnection(url, username, password);
            // 创建Statement对象
            stmt = conn.createStatement();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            // 关闭数据库连接
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
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
        LOGGER.debug("connect is init finished");
    }


    public static void userAccount() {
        String countSql = "SELECT count(*) FROM user_account";
        String sql = "SELECT id,wallet_no FROM user_account WHERE wallet_no!=\"\"";
        AtomicReference<ResultSet> rs = null;
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("indexName");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("indexName");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            LOGGER.debug("start read database ");
            rs.set(stmt.executeQuery(countSql));
            while (rs.get().next()) {
                int count = rs.get().getInt("count");
                int start = 0;
                while (count / MAX_VALUE != 0) {
                    EXECUTOR.execute(() -> {
                        String s = sql + "  'LIMIT' " + start + "," + MAX_VALUE;
                        try {
                            rs.set(stmt.executeQuery(s));
                            BulkRequest bulkRequest = new BulkRequest();
                            while (rs.get().next()) {
                                String id = rs.get().getString("id");
                                String walletNo = rs.get().getString("wallet_no");
                                WalletNoAndUserIdEntity entity = new WalletNoAndUserIdEntity();
                                entity.setUserId(id);
                                entity.setWalletNo(walletNo);
                                IndexRequest request = buildIndexRequest(entity, "", id);
                                bulkRequest.add(request);
                            }
                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        } catch (SQLException throwables) {
                            LOGGER.error(" error   ");
                        } catch (IOException e) {
                        }

                    });


                }
            }
            LOGGER.debug("database is read over");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        } finally {
            if (rs.get() != null) {
                try {
                    rs.get().close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    public static void walletAccountRecord() {
        String countSql = "SELECT count(*) FROM wallet_account_record";
        String sql = "SELECT id,wallet_no FROM wallet_account_record";
        AtomicReference<ResultSet> rs = null;
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("indexName");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("indexName");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            LOGGER.debug("start read database ");
            rs.set(stmt.executeQuery(countSql));
            while (rs.get().next()) {
                int count = rs.get().getInt("count");
                int start = 0;
                while (count / MAX_VALUE != 0) {
                    EXECUTOR.execute(() -> {
                        String s = sql + "  'LIMIT' " + start + "," + MAX_VALUE;
                        try {
                            rs.set(stmt.executeQuery(s));
                            BulkRequest bulkRequest = new BulkRequest();
                            while (rs.get().next()) {
                                String id = rs.get().getString("id");
                                String walletNo = rs.get().getString("wallet_no");
                                WalletNoAndUserIdEntity entity = new WalletNoAndUserIdEntity();
                                entity.setUserId(id);
                                entity.setWalletNo(walletNo);
                                IndexRequest request = buildIndexRequest(entity, "", id);
                                bulkRequest.add(request);
                            }
                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        } catch (SQLException throwables) {
                            LOGGER.error(" error   ");
                        } catch (IOException e) {
                        }

                    });


                }
            }
            LOGGER.debug("database is read over");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        } finally {
            if (rs.get() != null) {
                try {
                    rs.get().close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }

    public static void payRecord() {
        String countSql = "SELECT count(*) FROM pay_record";
        String sql = "SELECT id,wallet_no FROM pay_record";
        AtomicReference<ResultSet> rs = null;
        // 执行SQL语句
        try {
            GetIndexRequest indexRequest = new GetIndexRequest("indexName");
            boolean exists = client.indices().exists(indexRequest, RequestOptions.DEFAULT);
            if (!exists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("indexName");
                client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
            }
            LOGGER.debug("start read database ");
            rs.set(stmt.executeQuery(countSql));
            while (rs.get().next()) {
                int count = rs.get().getInt("count");
                int start = 0;
                while (count / MAX_VALUE != 0) {
                    EXECUTOR.execute(() -> {
                        String s = sql + "  'LIMIT' " + start + "," + MAX_VALUE;
                        try {
                            rs.set(stmt.executeQuery(s));
                            BulkRequest bulkRequest = new BulkRequest();
                            while (rs.get().next()) {
                                String id = rs.get().getString("id");
                                String walletNo = rs.get().getString("wallet_no");
                                WalletNoAndUserIdEntity entity = new WalletNoAndUserIdEntity();
                                entity.setUserId(id);
                                entity.setWalletNo(walletNo);
                                IndexRequest request = buildIndexRequest(entity, "", id);
                                bulkRequest.add(request);
                            }
                            client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        } catch (SQLException throwables) {
                            LOGGER.error(" error   ");
                        } catch (IOException e) {
                        }

                    });


                }
            }
            LOGGER.debug("database is read over");
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (IOException e) {

        } finally {
            if (rs.get() != null) {
                try {
                    rs.get().close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
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



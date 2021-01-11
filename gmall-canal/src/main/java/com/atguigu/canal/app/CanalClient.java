package com.atguigu.canal.app;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.atguigu.Constants.GmallConstants;
import com.atguigu.canal.utils.MyKafkaSender;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**作用：获取canal 连接，解析binlog，发给kafka
 * 1.获取canal连接
 * 2. 抓取message，一个message 有多个entry（也就是sql执行结果），一个sql命令 会对多行数据造成影响
 *      （1）while循环 为了不停的抓取
 *      （2）获取连接: 从哪个表中 抓取数据message.get(多少条)；判entrysize是否为空
 *      （3）获取entry（一个message有多个entry），判断rowdata
 *      （4）获取表名，序列化，反序列化，获取时间 类型，获取数据集合
 *      （5）根据需求（获取新增）： 造一个handler方法处理此数据，需要事件类型为insert 的
 * 3.handler 根据表名 及事件类型 处理数据 rowdatalist
 *  （1）判断是不是 order_info 和 事件类型insert --> 因为是新增数据， 需求是：交易总额，所以要今天新insert 进来的数据
 *  （2）rowdatalist 是list 遍历 --> rowdata 再遍历 得到columns，columns 要放在jsonobject 里面，因为最终要方法到kafka 里面
 *  （3）MyKafkaSender.send（分区，数据） 数据就是jsonobject.toString.
 * @author lance
 * @create 2020-11-06 16:08
 */
public class CanalClient {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        //1. 获取canal 连接，这里是连接canal，不是MySql 连接canal
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop102", 11111),
                "example",
                "",
                ""
        );

        //2. 抓取数据解析
        while (true){

            canalConnector.connect();           //获取一次数据，连接一次

            canalConnector.subscribe("gmall2020.*");     //指定消费的数据库里的 所有表

            Message message = canalConnector.get(100);    //抓取message

            if (message.getEntries().size()<=0) {
                System.out.println("当前没数据！休息一会");
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                //message 不为空，获取entries
                List<CanalEntry.Entry> entries = message.getEntries();

                for (CanalEntry.Entry entry : entries) { //遍历 entries 不止一个，是个list

                    CanalEntry.EntryType entryType = entry.getEntryType(); //获取entry type类型

                    if(CanalEntry.EntryType.ROWDATA.equals(entryType)){

                        String tableName = entry.getHeader().getTableName(); //获取表名

                        ByteString storeValue = entry.getStoreValue(); //获取序列化数据

                        //通过rowChange 反序列化,这里应该try-catch 捕获异常，为了代码 看得方便，才抛了异常
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(storeValue);

                        CanalEntry.EventType eventType = rowChange.getEventType(); //获取事件类型

                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList(); //获取 数据
                        
                        handler(tableName,eventType,rowDatasList);

                    }
                }
            }
        }
    }

    /**
     * 根据表名以及事件类型处理数据rowDatasList
     * @param tableName
     * @param eventType
     * @param rowDatasList
     */
    private static void handler(String tableName, CanalEntry.EventType eventType, List<CanalEntry.RowData> rowDatasList) {
        //订单表 只需要 tablename 是 order_info 表 和 事件类型（insert）-->因为是新增数据, 需求是：交易总额，所以要今天新insert 进来的数据
        // 已经存在的 放在前面，如果eventType.equals(INSERT) 可能会空指针异常
        if("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){

            sendToKafka(rowDatasList,GmallConstants.KAFKA_TOPIC_ORDER_INFO);

        }else if("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){

            sendToKafka(rowDatasList,GmallConstants.KAFKA_TOPIC_ORDER_DETAIL);

        }else if("user_info".equals(tableName) && (CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType))){

            sendToKafka(rowDatasList,GmallConstants.KAFKA_TOPIC_USER_INFO);
        }
    }

    //封装代码：去重代码冗余，大量重复代码
    private static void sendToKafka(List<CanalEntry.RowData> rowDatasList,String topic) {
        //遍历rowdatalist，有多个rowdata
        for (CanalEntry.RowData rowData : rowDatasList) {

            //一个rowdata 里多个列，这里创建JSON对象 存放 多个列的数据,后面好放在 kafka 里面
            JSONObject jsonObject = new JSONObject();

            //一个rowdata 有多个 列，所以要变遍历,这里binlog数据格式为 row
            // 应该是更改后的来的数据，但是 可能存在 更改之前的数据，这里选择更改之后的数据
            for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                jsonObject.put(column.getName(),column.getValue());   //kafka数据是 k，v类型
            }

            System.out.println(jsonObject.toString());

            //发送数据 至 kafka,
            MyKafkaSender.send(topic,jsonObject.toString());
        }
    }


}

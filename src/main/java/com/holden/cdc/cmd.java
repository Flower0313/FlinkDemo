package com.holden.cdc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.holden.bean.SensorReading;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @ClassName FlinkDemo-cmd
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2022年6月08日14:24 - 周三
 * @Describe
 */
public class cmd {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(1);
        DataStreamSource<String> source = env.fromElements(new String("{\"date\":\"19195\",\"codeNight\":\"1\",\"lowTemp\":\"18\",\"highTemp\":\"26\",\"cityId\":\"WRNJZW4PEF1G\",\"newUid\":{\"0\":{\"CONTACTPERSON\":\"\",\"ISDELETE\":\"0\",\"UPDATETIME\":\"1640053844000\",\"CREATEUSERNAME\":\"内蒙古自治区分行系统管理员\",\"CITYCODE\":\"150621\",\"UPDATEUSERUID\":\"8143758686505758720\",\"LONGT\":\"\",\"CREATEUSERUID\":\"8143758686505758720\",\"UPDATEUSERNAME\":\"内蒙古自治区分行系统管理员\",\"NAME\":\"农贸金融便利店\",\"UID\":\"8161459725239808000\",\"CODE\":\"0cce1de3-53a1-4891-830a-c585ee79629c\",\"ORGTYPE\":\"300\",\"ADDRESS\":\"\",\"PARENTUID\":\"8146310402876731461\",\"CONTACTTEL\":\"\",\"CREATETIME\":\"1640053844000\",\"PLATCODE\":\"3100\",\"LAT\":\"\",\"HIERARCHY\":\"700\",\"ANCESTORSORGID\":\"\"},\"1\":{\"CONTACTPERSON\":\"\",\"ISDELETE\":\"0\",\"UPDATETIME\":\"1645165092000\",\"CREATEUSERNAME\":\"系统管理员\",\"CITYCODE\":\"150621\",\"UPDATEUSERUID\":\"8143758686505758720\",\"LONGT\":\"\",\"CREATEUSERUID\":\"8143758686505758720\",\"UPDATEUSERNAME\":\"系统管理员\",\"NAME\":\"达南路离行自助银行\",\"UID\":\"8182897849786658816\",\"CODE\":\"41a397bc-ca25-4aef-bc8c-21440f9dc3b3\",\"ORGTYPE\":\"400\",\"ADDRESS\":\"\",\"PARENTUID\":\"8146310402876731459\",\"CONTACTTEL\":\"\",\"CREATETIME\":\"1645165092000\",\"PLATCODE\":\"3100\",\"LAT\":\"\",\"HIERARCHY\":\"700\",\"ANCESTORSORGID\":\"\"}},\"cityCode6\":\"150621\",\"codeDay\":\"13\"}"));


        SingleOutputStreamOperator<JSONObject> map = source.map(JSON::parseObject);

        map.flatMap(new FlatMapFunction<JSONObject, JSONObject>() {
            @Override
            public void flatMap(JSONObject value, Collector<JSONObject> out) throws Exception {
                JSONObject newUid = value.getJSONObject("newUid");
                for (int i = 0; i < newUid.size(); i++) {
                    String uid = newUid.getJSONObject(String.valueOf(i)).getString("UID");
                    value.put("uid", uid);
                    out.collect(value);
                }
            }
        }).print();


        env.execute();
    }
}

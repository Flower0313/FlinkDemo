package com.atguigu.sink;

import com.atguigu.source.SensorReading;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
;
import com.alibaba.fastjson.JSON;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * @ClassName FlinkDemo-sink_es_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月05日11:48 - 周日
 * @Describe
 */
public class sink_es_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200, "http"));


        dataStream.addSink(new ElasticsearchSink.Builder<SensorReading>(httpHosts, new ElasticsearchSinkFunction<SensorReading>() {
            @Override
            public void process(SensorReading sensorReading, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                //1.创建es写入请求
                IndexRequest request = Requests.indexRequest("sensor")
                        .type("_doc")
                        .id(sensorReading.getId())//这里id有相同的,但es上有幂等性,所以一个id只会存一次
                        .source(JSON.toJSONString(sensorReading), XContentType.JSON);
                //2.写入
                requestIndexer.add(request);
            }
        }).build());


        //批量请求的配置;这指示接收器在每个元素之后发出，否则它们将被缓冲
        //esSinkBuilder.setBulkFlushMaxActions(1);

        env.execute();
    }
}

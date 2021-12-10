package com.atguigu.trans;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @ClassName FlinkDemo-trans_select_4
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月21日13:50 - 周日
 * @Describe 分流操作，以30度为界，再合流
 */
public class trans_connect_union_4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\sensor.txt";

        DataStream<String> inputStream = env.readTextFile(inputPath);

        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        //声明标记
        OutputTag<SensorReading> highTag = new OutputTag<SensorReading>("high") {
        };
        OutputTag<SensorReading> lowTag = new OutputTag<SensorReading>("low") {
        };
        //1.进行分流
        SingleOutputStreamOperator<SensorReading> mainDataStream = dataStream
                .process(new ProcessFunction<SensorReading, SensorReading>() {
                    @Override
                    public void processElement(SensorReading sr,
                                               Context context,
                                               Collector<SensorReading> out) throws Exception {
                        if (sr.getTemperature() > 30) { //大于30度打上高温标记
                            context.output(highTag, sr);
                        } else {
                            context.output(lowTag, sr);
                        }
                    }
                });

        //选择分流
        DataStream<SensorReading> highStream = mainDataStream.getSideOutput(highTag);
        DataStream<SensorReading> lowStream = mainDataStream.getSideOutput(lowTag);

        //2.进行合流connect，将高温流转成二元组类，与低温流连接合并之后，输出状态信息
        DataStream<Tuple2<String, Double>> warningStream = highStream.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(SensorReading value) throws Exception {
                return new Tuple2<>(value.getId(), value.getTemperature());
            }
        });

        //用高温流连接低温流,底层this就是高温流，形参dataStream就是低温流
        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStreams = warningStream.connect(lowStream);
        //CoMapFunction的输入数据类型和ConnectedStreams相同，返回的是Object，所以返回类型随你需求来变
        connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {
            /*
             * Attention!!!
             * 两条流互不干扰，各自处理各自的数据，点进去CoMapFunction源码,
             * 能看见map1和map2分别对应CoMapFunction的第一个和第二个泛型,
             * 也就是mqp1处理调用connect方法的流,map2处理connect()参数中的流,而这两个map的返回的类型都可以不同,
             * 所以在CoMapFunction中第三个参数返回值类型的时候需要定义map1和map2共用的返回类型,这里也就是Object
             * */
            @Override//为第一个连接流中的每个元素调用此方法
            public Object map1(Tuple2<String, Double> value) throws Exception {
                //超过30度就返回三元组，最后是报警信息
                return new Tuple3<>(value.f0, value.f1, "high temp warning!");
            }

            @Override//为第二个连接流中的每个元素调用此方法。
            public Object map2(SensorReading value) throws Exception {
                //没超过就正常
                return new Tuple2<>(value.getId(), "normal");
            }
        }).print();

        //3.或者使用union来合流
        //highStream.union(lowStream);


        env.execute();
    }
}



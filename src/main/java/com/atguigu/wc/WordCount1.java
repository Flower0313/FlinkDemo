package com.atguigu.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;


/**
 * @ClassName FlinkDemo-WordCount
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年11月19日16:28 - 周五
 * @Describe flink之批处理WordCount程序
 */
public class WordCount1 {
    public static void main(String[] args) throws Exception {
        //todo 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //todo 从文件读取数据
        String inputPath = "T:\\ShangGuiGu\\FlinkDemo\\src\\main\\resources\\wc.txt";

        //todo 读取数据
        //DataSource中本质就是DataSet
        DataSource<String> dataSource = env.readTextFile(inputPath);

        DataSet<Tuple2<String,Integer>> wordCountDataSet=
                dataSource.flatMap(new MyFlatMapper())
                        .groupBy(0) //按照第一个位置的word分组
                        .sum(1);//将第二个位置上的数据求和
        wordCountDataSet.print();
    }


    //自定义类，声明输入类型和输出类型
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out)
                throws Exception {
            //拿到一行行value,按空格进行拆分
            String[] words = value.split(" ");
            for (String word : words) {
                out.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}



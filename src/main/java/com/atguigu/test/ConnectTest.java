package com.atguigu.test;

import com.atguigu.bean.TestClass;
import com.atguigu.bean.TestStudent;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

import static com.atguigu.common.CommonEnv.ORDER_SOURCE;
import static com.atguigu.common.CommonEnv.RECEIPT_SOURCE;

/**
 * @ClassName FlinkDemo-Connect_Test
 * @Author Holden_—__——___———____————_____Xiao
 * @Create 2021年12月08日19:38 - 周三
 * @Describe
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(10);

        //step-1 读取Order流
        DataStream<TestStudent> StudentDS = env.readTextFile("input/Student.txt")
                .map(line -> {
                    String[] data = line.split(",");
                    return new TestStudent(
                            String.valueOf(data[0]),
                            String.valueOf(data[1]),
                            String.valueOf(data[2])
                    );
                });

        //step-2 读取Receipt流
        DataStream<TestClass> ClassDS = env.readTextFile("input/Class.txt")
                .map(line -> {
                    String[] data = line.split(",");
                    return new TestClass(
                            String.valueOf(data[0]),
                            String.valueOf(data[1])
                    );
                });

        //step-3 将两个流连接在一起
        ConnectedStreams<TestStudent, TestClass> AllStudentCS = StudentDS.connect(ClassDS);

        //step-4 因为不同的数据流到达的先后顺序不同,所以需要匹配对账信息,输出表示对账成功与否

        AllStudentCS
                .keyBy("cId", "cId")
                .process(new StudentFunction())
                .print();
        /*
         * Prepare-1:
         *   1.准备TestStudent和TestClass两个类,里面的join字段是cId
         *   2.将并行度设置为5
         *   3.将两个流connect后对cId进行keyBy
         *
         * Result-1:
         *   1> 4-四班
         *   5> 1-一班
         *   3> 3-三班
         *   3> 2-二班
         *   5> 1-1001-肖华
         *   5> 1-1003-李超凡
         *   3> 3-1004-zzx
         *   3> 2-1002-flower
         *   1> 4-1005-周子雄
         *   3> 2-1006-彭奎奎
         *
         * Conclusion-1:
         *   1.两条流中同cId的数据会分到同一个分区
         *   2.若TestClass中有两个cId一样的2号,那么与TestStudent关联时不会产生笛卡尔积,还是只输出两个Student
         *
         ***********************************************************************************************
         ************************************************************************************************
         *
         * Prepare-2:
         *   1.准备TestStudent和TestClass两个类,里面的join字段是cId
         *   2.将并行度设置为5
         *   3.将两条流的值通过HashMap逻辑关联在一起
         *
         * Result-1:
         *   3> B流 - 2-二班一号-1002-flower
         *   5> A流 - 1-一班-1001-肖华
         *   3> B流 - 3-三班-1004-zzx
         *   5> A流 - 1-一班-1003-李超凡
         *   3> B流 - 2-二班二号-1006-彭奎奎
         *   1> A流 - 4-四班-1005-周子雄
         *
         * Conclusion-1:
         *   1.两条流中同cId的数据会分到同一个分区
         *   2.可以看到二班不会有笛卡尔积
         *   3.这里出现了两个二班是因为后面的二班二号进来就将二班一号覆盖了
         * */

        env.execute();
    }

    public static class StudentFunction extends CoProcessFunction<TestStudent, TestClass, Object> {
        Map<String, TestStudent> studentMap = new HashMap<>();

        Map<String, TestClass> classMap = new HashMap<>();

        @Override
        public void processElement1(TestStudent stu, CoProcessFunction<TestStudent, TestClass, Object>.Context ctx, Collector<Object> out) throws Exception {
            if (classMap.containsKey(stu.getCId())) {
                out.collect("A流 - " + stu.getCId() + "-" + classMap.get(stu.getCId()).getCName() + "-" + stu.getSId() + "-" + stu.getSName());
                /*
                 * Q&A!
                 *  Q1:分配好的学生就丢出去,为什么班级不丢呢?
                 *  A1:因为学生的粒度更小,学生和班级的对应关系是n:1
                 * */
                studentMap.remove(stu.getCId());
            } else {
                studentMap.put(stu.getCId(), stu);
            }
        }

        @Override
        public void processElement2(TestClass cls, CoProcessFunction<TestStudent, TestClass, Object>.Context ctx, Collector<Object> out) throws Exception {
            if (studentMap.containsKey(cls.getCId())) {
                out.collect("B流 - " + cls.getCId() + "-" + cls.getCName() + "-" + studentMap.get(cls.getCId()).getSId() + "-" + studentMap.get(cls.getCId()).getSName());
            } else {
                classMap.put(cls.getCId(), cls);
            }
        }
    }

}

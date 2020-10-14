package myflink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableEnvironment;
//import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import static org.apache.flink.table.api.Expressions.$;


public class Main {

    static String file_path = "./src/main/java/myflink/homeAssignment.csv";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment stream_env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamKeyPartitionerSink sinkFunction = new StreamKeyPartitionerSink(
                "./data/key_grouping", "f0"); // f0 is the key field name

        DataStream<Tuple2<String, String>> stream_input = stream_env.readTextFile(file_path).map((MapFunction<String, Tuple2<String, String>>) s -> {
            String[] cells = s.split(",");
            return new Tuple2<String, String>(cells[0], cells[1]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING));

        Iterator<Tuple2<String, String>> myOutput = DataStreamUtils.collect(stream_input);
        List<Tuple2<String, String>> list = new ArrayList<>();
        while (myOutput.hasNext())
            list.add(myOutput.next());
        list.sort(Comparator.comparingInt(v -> Integer.parseInt(v.f1)));

        DataStream<Tuple2<String, String>> results = stream_env.fromCollection(list).returns(Types.TUPLE(Types.STRING, Types.STRING));
        KeyedStream<Tuple2<String, String>, Tuple> keyedDataStream = results.keyBy(0);
        keyedDataStream.addSink(sinkFunction);
        stream_env.execute();
    }
}

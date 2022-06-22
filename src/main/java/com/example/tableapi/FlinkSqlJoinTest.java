package com.example.tableapi;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @author liuwe 2022/6/22 9:52
 */
public class FlinkSqlJoinTest {
    public static void main(String[] args) {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // inner join两张表默认的TTL是PT0S(process time 0 second),即默认不过期
//        System.out.println(tableEnvironment.getConfig().getIdleStateRetention());
        // 生产环境一般都要配置TTL。设置两张表的TTL为10s
        tableEnvironment.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        DataStream<Pojo1> pojo1Stream = environment.socketTextStream("master1", 6666).map(str -> {
            String[] splits = str.split(",");
            return new Pojo1(splits[0], splits[1], Long.parseLong(splits[2]));
        });

        DataStream<Pojo2> pojo2Stream = environment.socketTextStream("master1", 8888).map(str -> {
            String[] splits = str.split(",");
            return new Pojo2(splits[0], splits[1], Long.parseLong(splits[2]));
        });

        tableEnvironment.createTemporaryView("t1", pojo1Stream);
        tableEnvironment.createTemporaryView("t2", pojo2Stream);

        // 更新TTL策略:
        // 内连接      左表: UpdateType.OnCreateAndWrite        右表: UpdateType.OnCreateAndWrite
        /*tableEnvironment.sqlQuery("select t1.id,t1.name,t2.gender from t1 join t2 on t1.id = t2.id")
                .execute().print();*/

        // 左外连接     左表: UpdateType.OnReadAndWrite          右表: UpdateType.OnCreateAndWrite
        /*tableEnvironment.sqlQuery("select t1.id,t1.name,t2.gender from t1 left join t2 on t1.id = t2.id")
                .execute().print();*/

        // 右外连接     左表: UpdateType.OnCreateAndWrite        右表: UpdateType.OnReadAndWrite
        /*tableEnvironment.sqlQuery("select t2.id,t1.name,t2.gender from t1 right join t2 on t1.id = t2.id")
                .execute().print();*/


        // 全外连接三种写法：
        // 第一种：select * from a full join b on a.id=b.id full join c on a.id=c.id
        // 第二种：select * from a full join b on a.id=b.id full join c on b.id=c.id
        // 第三种：select * from a full join b on a.id=b.id full join c on coalesce(a.id,b.id)=c.id (hive的语法)
        // 另一种正确的写法：select * from a full join b on a.id=b.id full join c on nvl(a.id,b.id)=c.id
        // 一和二的结果可能会多, 三是正确的, 一、二的写法是前两张表先join，join出的结果与第三张表join
        // coalesce()函数可以使用nvl()函数代替, 但nvl()函数只限于两个字段
        // 第三种写法可是用left join代替, 而且left join运行要快
        /*
         * select a.id, b的字段, c的字段, d的字段
         * from a
         * left join b on a.id = b.id
         * left join c on a.id = c.id
         * left join d on a.id = d.id
         */
        // 全外连接     左表: UpdateType.OnReadAndWrite          右表: UpdateType.OnReadAndWrite
        tableEnvironment.sqlQuery("select t1.id,t2.id,t1.name,t2.gender from t1 full join t2 on t1.id = t2.id")
                .execute().print();

    }
}

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import kafka.utils.{ZKGroupTopicDirs, ZkUtils}
import org.I0Itec.zkclient.ZkClient
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Duration, StreamingContext}
import redis.clients.jedis.Jedis

object KafkaDirectConsumer {
  def main(args: Array[String]): Unit = {
    // 创建streaming
    val conf = new SparkConf().setAppName("demo").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Duration(5000))
    // 创建
    // 指定消费者组
    val groupid = "gp01"
    // 消费者
    val topic = "tt1"
    // 创建zk集群连接
    val zkQuorum = "spark101:2181,spark102:2181,spark103:2181"
    // 创建kafka的集群连接
    val brokerList = "spark101:9092,spark102:9092,spark103:9092"
    // 创建消费者的集合
    // 在streaming中可以同时消费多个topic
    val topics: Set[String] = Set(topic)
    // 创建一个zkGroupTopicDir对象
    // 此对象里面存放这zk组和topicdir的对应信息
    // 就是在zk中写入kafka的目录
    // 传入 消费者组，消费者，会根据传入的参数生成dir然后存放在zk中
    val TopicDir = new ZKGroupTopicDirs(groupid, topic)
    // 获取存放在zk中的dir目录信息 /gp01/offset/tt
    val zkTopicPath: String = s"${TopicDir.consumerOffsetDir}"
    // 准备kafka的信息、
    val kafkas = Map(
      // 指向kafka的集群
      "metadata.broker.list" -> brokerList,
      // 指定消费者组
      "group.id" -> groupid,
      // 从头开始读取数据
      "auto.offset.reset" -> kafka.api.OffsetRequest.SmallestTimeString
    )
    // 创建一个zkClint客户端，用host 和 ip 创建
    // 用于从zk中读取偏移量数据，并更新偏移量
    // 传入zk集群连接
    val zkClient = new ZkClient(zkQuorum)
    // 拿到zkClient后去，zk中查找是否存在文件
    // /gp01/offset/tt/0/10001
    // /gp01/offset/tt/1/20001
    // /gp01/offset/tt/2/30001
    val clientOffset = zkClient.countChildren(zkTopicPath)
    // 创建空的kafkaStream 里面用于存放从kafka接收到的数据
    var kafkaStream: InputDStream[(String, String)] = null
    // 创建一个存放偏移量的Map
    // TopicAndPartition [/gp01/offset/tt/0,10001]
    var fromOffsets: Map[TopicAndPartition, Long] = Map()
    // 判断，是否妇女放过offset，若是存放过，则直接从记录的
    // 偏移量开始读
    if (clientOffset > 0) {
      // clientOffset的数量就是 分区的数目量
      for (i <- 0 until clientOffset) {
        // 取出 /gp01/offset/tt/i/ 10001 -> 偏移量
        val paratitionOffset = zkClient.readData[String](s"${zkTopicPath}/${i}")
        // tt/ i
        val tp = TopicAndPartition(topic, i)
        // 添加到存放偏移量的Map中
        fromOffsets += (tp -> paratitionOffset.toLong)
      }
      // 现在已经把偏移量全部记录在Map中了
      // 现在读kafka中的消息
      // key 是kafka的kay,为null， value是kafka中的消息
      // 这个会将kafka的消息进行transform 最终kafka的数据都会变成（kafka的key，message）这样的tuple
      val messageHandlers = (mmd: MessageAndMetadata[String, String]) => (mmd.key(), mmd.message())
      // 通过kafkaUtils来创建DStream
      // String,String,StringDecoder,StringDecoder,(String,String)
      //   key,value,key的解码方式,value的解码方式,(接受的数据格式)
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
        ssc, kafkas, fromOffsets, messageHandlers
      )
    } else { // 若是不存在，则直接从头读
      // 根据kafka的配置
      kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkas, topics)
    }

    // 偏移量范围
    var offsetRanges = Array[OffsetRange]()

    kafkaStream.foreachRDD {
      kafkaRDD =>
        // 得到kafkaRDD，强转为HasOffsetRanges，获得偏移量
        // 只有Kafka可以强转为HasOffsetRanges
        offsetRanges = kafkaRDD.asInstanceOf[HasOffsetRanges].offsetRanges

        // 触发Action,这里去第二个值为真实的数据
        val mapRDD = kafkaRDD.map(_._2)
      /*=================================================*/ 　　　　　// mapRDD为数据，在这里对数据操作　　　　　// 在这里写你自己的业务处理代码代码　　　　　// 此程序可以直接拿来使用，经历过层层考验　　　　　/*=================================================*/

      // 存储更新偏移量
      for (o <- offsetRanges) {
        // 获取dir
        val zkPath = s"${zkTopicPath}/${o.partition}"
        ZkUtils.updatePersistentPath(zkClient, zkPath, o.untilOffset.toString)
      }
    }

    ssc.start()
    ssc.awaitTermination()

  }
}
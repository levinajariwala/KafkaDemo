//Challenge 2: Streaming Data Transformation
//Task
//Assume you're consuming streaming data representing user actions from a Kafka topic. Each message in the stream is a JSON object like the following:
//
//{"user_id": "1234", "action": "click", "timestamp": "2024-03-25T12:00:00Z", "page_id": "home"}
//
//Using Scala and Apache Spark Streaming:
//
//  Consume messages from a Kafka topic named user_actions.
//Filter out messages where the action is not "click".
//  Count the number of click actions per page_id in a 5-minute window.
//Output the count to the console or write it to another Kafka topic.
//  Considerations
//Consider windowing and state management in a streaming context.
//  Demonstrate your understanding of processing time vs. event time.
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class userKafkaConsumer{

}
object  userKafkaConsumer{
  def main(args:Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]").appName("test").getOrCreate()

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "ip-172-31-3-80.eu-west-2.compute.internal:9092",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "group.id" -> "group1",
      "auto.offset.reset" -> "earliest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val topic = "user_actions"

    val schema = StructType(Seq(
      StructField("user_id", StringType, nullable = false),
      StructField("action", StringType, nullable = false),
      StructField("timestamp", StringType, nullable = false),
      StructField("page_id", StringType, nullable = false)
    )
    )
    val df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "ip-172-31-3-80.eu-west-2.compute.internal:9092")
      .option("subscribe", topic)
      .option("startingOffsets", "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).as("data"))
      .selectExpr("data.*")

    df.show()

  }
}
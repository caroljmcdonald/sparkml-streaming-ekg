package com.sparkkafka.ekg

// http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams.html

import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.kafka09.{ ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.dstream.{  DStream }
import org.apache.spark.streaming.kafka.producer._
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors



/**
 * Consumes messages from a topic in MapR Streams using the Kafka interface,
 * enriches the message with  the k-means model cluster id and publishs the result in json format
 * to another topic
 * Usage: SparkKafkaConsumerProducer  <model> <topicssubscribe> <topicspublish>
 *
 *   <model>  is the path to the saved model
 *   <topics> is a  topic to consume from
 *   <topicp> is a  topic to publish to
 * Example:
 *    $  spark-submit --class com.sparkkafka.ekg.SparkKafkaConsumerProducer --master local[2] \
 * mapr-sparkml-streaming-ekg-1.0.jar /user/user01/data/savemodel  /user/user01/stream:ekgs /user/user01/stream:ekgp
 *
 *    for more information
 *    http://maprdocs.mapr.com/home/Spark/Spark_IntegrateMapRStreams_Consume.html
 *
 * private static KafkaProducer<String, String> getKafkaProducer() {
 * if (producer == null) {
 * producer = new KafkaProducer<>(Config.getConfig().getPrefixedProps("kafka."));
 * }
 * return producer;
 * }
 */

object SparkKafkaConsumerProducer extends Serializable {

  import org.apache.spark.streaming.kafka.producer._

    def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      throw new IllegalArgumentException("You must specify the model path, subscribe topic and publish topic. For example /user/user01/data/savemodel /user/user01/stream:ekgs /user/user01/stream:ekgp ")
    }

    val Array(modelpath, topics, topicp) = args
    System.out.println("Use model " + modelpath + " Subscribe to : " + topics + " Publish to: " + topicp)

    val brokers = "maprdemo:9092" // not needed for MapR Streams, needed for Kafka
    val groupId = "sparkApplication"

    val batchTime = Seconds(1)
    val pollTimeout = "10000"

    val sparkConf = new SparkConf().setAppName("ekgStream")
    val ssc = new StreamingContext(sparkConf, batchTime)

    val producerConf = new ProducerConf(
      bootstrapServers = brokers.split(",").toList
    )

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    // set up consumer properties
    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
      "spark.kafka.poll.time" -> pollTimeout,
      "spark.streaming.kafka.consumer.poll.ms" -> "8192"
    )
    // set up producer properties
    val pConfig = Map(
      ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      "retries" -> "0",
      "batch.size" -> "16384",
      "linger.ms" -> "1",
      "buffer.memory" -> "33554432",
      "enable.auto.commit" -> "true",
      "auto.commit.interval.ms" -> "1000",
      "session.timeout.ms" -> "30000"
    )
    val N = 32
    var window = for (i <- 0 to N - 1) yield math.pow(math.sin(math.Pi * i / (N - 1)), 2)

    def normalize(v: Array[Double]): Double = {
      var norm = 0.0
      for (i <- 0 to v.size - 1) { norm = norm + math.pow(v(i), 2) }
      math.sqrt(norm)
    }
    // load model for getting clusters

    val model = KMeansModel.load(ssc.sparkContext, modelpath)
    //model.clusterCenters.foreach(println)

    val clusterCenters = model.clusterCenters.map(_.toArray)
    // create kafka consumer direct stream
    val consumerStrategy = ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams)
    val messagesDStream = KafkaUtils.createDirectStream[String, String](
      ssc, LocationStrategies.PreferConsistent, consumerStrategy
    )
    // get message values from key,value
    val valuesDStream: DStream[String] = messagesDStream.map(_.value())

    valuesDStream.foreachRDD { rdd =>

      rdd.foreachPartition { prdd =>
     
        val producer = KafkaProducerFactory.getOrCreateProducer(pConfig)
        // create a vector for each message value in rdd
        val vrdd = prdd.map(line => Vectors.dense(line.split('\t').map(_.toDouble)))
        var previous = Array.fill[Double](N)(0.0)
        var cola = new Array[Double](N / 2)
        var colb = new Array[Double](N / 2)
        // for each vector in rdd
        vrdd.foreach { v =>
          // process input record 
          val w = Array.fill[Double](N)(0.0)
          for (i <- 0 to v.size - 1) { w(i) = v(i) * window(i) }
          val wnorm = normalize(w)
          val processed = Vectors.dense(w.map(_ / wnorm))
          // reconstruct & write to topic
          // get centroid & reconstruct
          val cluster = model.predict(processed)
          val center = clusterCenters(cluster)
          val current = center.map(_ * wnorm)
          var message = ""
          // reconstruct & write to topic
          for (i <- 0 to v.size / 2 - 1) {
            cola(i) = v(i)
            colb(i) = current(i)
          }
        
          message = "{\"xValue\":" + cluster + ",\"columnA\":" + cola.mkString("[", ", ", "]") + ",\"columnB\":" + colb.mkString("[", ", ", "]") + "}"
          val record = new ProducerRecord(topicp, "key", message)
          println(message)

          Thread.sleep(3000l);
          producer.send(record)
          previous = current
        }

      }
    }
    // Start the computation

    ssc.start()
    // Wait for the computation to terminate
    ssc.awaitTermination()

  }

}
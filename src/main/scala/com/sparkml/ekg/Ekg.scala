/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.sparkml.ekg

import org.apache.spark._
import org.apache.spark.rdd.RDD

import org.apache.spark.sql.types._
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.mllib.linalg.Vectors

import org.apache.spark.sql.SparkSession

object Ekg {

  def main(args: Array[String]) {
    //val spark = SparkSession.builder().appName("Clusterekg").getOrCreate()

    val conf = new SparkConf().setAppName("SparkDFebay")
    val sc = new SparkContext(conf)

    /// 1. read in data from stream and convert to vector of Doubles & read in centroids ...
    val rdd = sc.textFile("/user/user01/data/toReconstruct.tsv")
    val vrdd: org.apache.spark.rdd.RDD[org.apache.spark.mllib.linalg.Vector] = rdd.map(line => Vectors.dense(line.split('\t').map(_.toDouble)))
    val model = KMeansModel.load(sc, "/user/user01/data/anomaly-detection-master")
    val clusterCenters = model.clusterCenters.map(_.toArray)

    /// 2. window and normalize each record....
    val N = 32
    var window = for (i <- 0 to N - 1) yield math.pow(math.sin(math.Pi * i / (N - 1)), 2)

    def normalize(v: Array[Double]): Double = {
      var norm = 0.0
      for (i <- 0 to v.size - 1) { norm = norm + math.pow(v(i), 2) }
      math.sqrt(norm)
    }

    /// 3. reconstruct and write to topic
    // val test = vrdd.take(40)
    var previous = Array.fill[Double](N)(0.0)
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

      // reconstruct & write to topic
      for (i <- 0 to v.size / 2 - 1) {
        println(v(i), current(i) + previous(v.size / 2 + i), cluster)

      }

      previous = current
    }

  }

}


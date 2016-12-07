/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging

/** Simulates a long-running computation with a delay inserted into a map */
object DelayedMap extends Logging {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("Delayed Map")
      .getOrCreate()
    // delay in milliseconds
    val delay = args(0).toInt
    // remaining args are numbers of partitions to use
    val plist = args.drop(1).map(_.toInt)

    plist.foreach { np =>
      logInfo(s"DELAYED MAP: np= $np")
      spark.sparkContext.parallelize(1 to 1000, np).map { x =>
        Thread.sleep(delay)
        x
      }.count
    }

    spark.stop()
  }
}
// scalastyle:on println

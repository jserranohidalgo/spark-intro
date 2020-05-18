package com.company.project
package main

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import pureconfig._

import pipeline.{PipelineWorkflow, PipelineError}

object Main extends App {

  def runPipeline(args: Array[String])(
      implicit spark: SparkSession): Either[PipelineError, Unit] =
    for {
      config <- Args.load(args, ConfigSource.default).right
      _ <- new PipelineWorkflow(config.inputPath, config.outputPath).run.right
    } yield ()

  val logRun = LoggerFactory.getLogger(getClass.getName)
  implicit val spark = SparkSession.builder().master("local[*]").getOrCreate()

  runPipeline(args) match {
    case Left(error) =>
      throw new RuntimeException(s"Error running pipeline $error: ${error.msg}")
    case _ => logRun.info("Pipeline executed successfully.")
  }

  spark.stop
}

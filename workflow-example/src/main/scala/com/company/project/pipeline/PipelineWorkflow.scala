package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import com.company.project._

import scala.util.Try

class PipelineWorkflow(inputPath: String, outputPath: String) {

  def run(implicit spark: SparkSession): Either[PipelineError, Unit] = {
    for {
      input <- readData().right
      transformedData <- Transformations.goalsPerTeam(input).right
      res <- writeData(transformedData).right
    } yield res
  }

  def readData()(implicit spark: SparkSession): Either[ReadError, DataFrame] =
    Try(spark.read.parquet(inputPath))
      .toEither(error => ReadError(error.getMessage))

  def writeData(df: DataFrame): Either[ExecutionError, Unit] =
    Try(df.write.mode(SaveMode.Overwrite).parquet(outputPath))
      .toEither(error => ExecutionError(error.getMessage))


}

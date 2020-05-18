package com.company.project.pipeline

import org.apache.spark.sql.{DataFrame, functions => func}
import com.company.project._
import scala.util.Try

object Transformations {

  def goalsPerTeam(input: DataFrame): Either[PipelineError, DataFrame] =
    Try{
      import input.sparkSession.implicits._

      input.select(
        func.explode(
          func.array(
            func.struct(
              $"home".alias("team"),
              $"homegoals".alias("goals")
            ),
            func.struct(
              $"away".alias("team"),
              $"awayGoals".alias("goals")
            )
          )
        ).alias("value")
      ).selectExpr("value.*")
       .groupBy($"team")
       .agg(func.sum($"goals").alias("goals"))
    }.toEither(e => TransformationError(e.getMessage))

}

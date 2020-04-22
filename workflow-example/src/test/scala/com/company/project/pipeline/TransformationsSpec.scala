package com.company.project.pipeline

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.scalatest.FunSpec

import Model._

class TransformationsSpec extends FunSpec with DataFrameSuiteBase {

  import spark.implicits._

  describe("A transformation") {

    it("a dataframe is processed") {

      val matchesDF = List(
        Match("Barcelona", "Real Madrid", Some(2), Some(2)),
        Match("Osasuna", "Real Madrid", Some(2), Some(0)),
        Match("Real Madrid", "Betis", Some(2), None)
      ).toDF

      val expectedDF = List(
        GoalsPerTeam("Osasuna", Some(2)),
        GoalsPerTeam("Real Madrid", Some(4)),
        GoalsPerTeam("Barcelona", Some(2)),
        GoalsPerTeam("Betis", None)
      ).toDF

      Transformations.goalsPerTeam(matchesDF).fold(
        fail(_),
        obtainedDF =>
          assertDataFrameEquals(expectedDF.sort($"team"), obtainedDF.sort($"team"))
      )
    }
  }
}

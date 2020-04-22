package com.company.project.main

import com.company.project.pipeline.ConfigError
import pureconfig._
import pureconfig.generic.auto._

case class Args(inputPath: String, outputPath: String)

object Args {
  private val argsParser = new scopt.OptionParser[Args]("My Spark pipeline") {
    head("My Spark pipeline", "1.x")

    opt[String]('i', "input")
      .optional()
      .action((x, c) => c.copy(inputPath = x))
      .text("Input path")

    opt[String]('o', "output")
      .optional()
      .action((x, c) => c.copy(outputPath = x))
      .text("Output path")
  }

  private[main] def load(args: Array[String], configSource: ConfigObjectSource) =
    for {
      defaultConfig <- configSource.load[Args].left
                          .map(e => ConfigError(e.head.description)).right
      config <- argsParser.parse(args, defaultConfig)
                  .toRight(ConfigError("Unable to parse configuration")).right
    } yield config
}

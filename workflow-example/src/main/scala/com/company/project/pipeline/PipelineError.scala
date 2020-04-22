package com.company.project.pipeline

sealed trait PipelineError extends Throwable {
  def msg: String
}

case class ConfigError(msg: String) extends PipelineError

case class ReadError(msg: String) extends PipelineError

case class ExecutionError(msg: String) extends PipelineError

case class TransformationError(msg: String) extends PipelineError
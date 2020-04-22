package com.company

import scala.util.{Failure, Success, Try}

package object project {

  implicit class TryOps[A](tryObj: Try[A]) {
    def toEither[L](left: Throwable => L): Either[L, A] =
      tryObj match {
        case Success(a) => Right(a)
        case Failure(e) => Left(left(e))
      }
  }
}

package com.company.project.pipeline

object Model{

  case class Match(home: String,
    away: String,
    homeGoals: Option[Long],
    awayGoals: Option[Long])

  case class GoalsPerTeam(team: String,
    goals: Option[Long])
}

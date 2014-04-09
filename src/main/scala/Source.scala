package stretchypants

import org.json4s.{ JArray, JBool, JString, JValue }
import org.json4s.JsonDSL._

sealed trait Source {
  def asJson: JValue
}

object Source {
  case object None extends Source {
    def asJson = JBool(false)
  }
  case class Include(include: List[String]) extends Source {
    def asJson = JArray(include.map(JString(_)))
  }
  case class Mixed(include: List[String], exclude: List[String]) extends Source {
    def asJson = ("include" -> include) ~ ("exclude" -> exclude)
  }
}

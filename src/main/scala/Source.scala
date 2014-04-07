package stretchypants

sealed trait Source

object Source {
  case object None extends Source
  case class Include(include: List[String]) extends Source
  case class Mixed(include: List[String], exclude: List[String]) extends Source
}

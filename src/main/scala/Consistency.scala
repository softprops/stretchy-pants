package stretchypants

sealed trait Consistency {
  def value: String
}

object Consistency {
  abstract class Value(val value: String) extends Consistency
  case object One extends Value("one")
  case object Quorum extends Value("quorum")
  case object All extends Value("all")
}

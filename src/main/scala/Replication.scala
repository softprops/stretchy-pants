package stretchypants

sealed trait Replication {
  def value: String
}

object Replication {
  abstract class Value(val value: String) extends Replication
  case object Async extends Value("async")
  case object Sync extends Value("sync")
}

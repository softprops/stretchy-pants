# stretchy pants

> An elastic wasteband for text

Stretchy pants is a flexible non blocking interface for querying elastic search in scala.

# usage

The interface is currently a work in process

## doc/indexing

```scala
import stretchypants._, dispatch._
import scala.concurrent.ExecutionContext.Implicits.global
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods.{ compact, render }

// initialize client
val es = Client(host)

// initialize an indexer
val tweeter = es.index("twitter", "tweet")

// create some tweets
tweeter(compact(render(
  ("content" -> "stretchy pants!") ~
  ("user" -> ("name" -> "@stretchy"))))).id("1")(as.String).onComplete(println)
tweeter(compact(render(("content" -> "stretchy") ~
  ("user" -> ("name" -> "@pants"))))).id("2")(as.String).onComplete(println)
tweeter(compact(render(("content" -> "pants") ~
  ("user" -> ("name" -> "@pantz"))))).id("3")(as.String).onComplete(println)
  
  
// get tweets one by one
val tweeting = es.get("twitter", "tweet")
(1 to 3).foreach(id => tweeting(id.toString)(as.String).onComplete(println))

// get multiple tweets in one swoop
val multiTweeting = es.multiGet.index("twitter").kind("tweet")
multiTweeting.ids((1 to 3).map(_.toString):_*)(as.String).onComplete(println)

// delete tweets one by one
val detweet = es.delete("twitter", "tweet")
(1 to 3).foreach(id => detweet(id.toString)(as.String).onComplete(println))

// or delete tweets by query
val multiDetweet = es.deleteQuery(Query.matchall)
  .index("twitter")
  .kind("tweet")(as.String).onComplete(println)
```


## searching

```scala
import stretchypants._, dispatch._
import scala.concurrent.ExecutionContext.Implicits.global

// initialize client
val es = Client(host)

// initialize a search request
val tweets = es.search("twitter", "tweet")

// query all tweets
tweets(Query.matchall)(as.String).onComplete(println)

// search for the something like stretch in user.name and content fields of a tweet
tweets(Query.fuzzy("stretchy").fields("user.name", "content"))(as.String).onComplete(println)

// search for "stretchy" or "pants" in a field named content
val query = Query.matching(("content", "stretchy pants"))
tweets(query)(as.String).onComplete(println)

// search for "stretchy" and "pants" in a field named content
tweets(query.and)(as.String).onComplete(println)

// search for the phrase "stretchy pants" in a field named content
tweets(query.phrase)(as.String).onComplete(println)
```

Doug Tangren (softprops) 2014

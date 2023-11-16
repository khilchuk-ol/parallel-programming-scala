package reductions

import scala.annotation.*
import org.scalameter.*

object ParallelParenthesesBalancingRunner:

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns := 40,
    Key.exec.maxWarmupRuns := 80,
    Key.exec.benchRuns := 120,
    Key.verbose := false
  ) withWarmer(Warmer.Default())

  def main(args: Array[String]): Unit =
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime")
    println(s"speedup: ${seqtime.value / fjtime.value}")

object ParallelParenthesesBalancing extends ParallelParenthesesBalancingInterface:

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def iterate(c: Array[Char], parenthesis: Int): Int = {
      if (parenthesis < 0 || c.isEmpty) parenthesis
      else if (c.head == '(') iterate(c.tail, parenthesis + 1)
      else if (c.head == ')') iterate(c.tail, parenthesis - 1)
      else iterate(c.tail, parenthesis)
    }

    if (chars.isEmpty) true
    else iterate(chars, 0) == 0
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, arg1: Int, arg2: Int): Int = {
      if (idx == until) arg1 - arg2
      else if (chars(idx) == '(') traverse(idx + 1, until, arg1 + 1, arg2)
      else if (chars(idx) == ')') traverse(idx + 1, until, arg1, arg2 + 1)
      else traverse(idx + 1, until, arg1, arg2)
    }

    def reduce(from: Int, until: Int): Int = {
      val length = until - from
      if (length / 4 >= threshold) {
        val par = parallel(
          traverse(from, threshold, 0, 0),
          traverse(from + threshold, threshold * 2, 0, 0),
          traverse(from + threshold * 2, threshold * 3, 0, 0),
          traverse(from * threshold * 3, until, 0, 0)
        )
        par._1 + par._2 + par._3 + par._4
      } else if ((until - from) / 2 >= threshold) {
        val par = parallel(
          traverse(from, threshold, 0, 0),
          traverse(from + threshold, until, 0, 0)
        )
        par._1 + par._2
      } else {
        traverse(from, until, 0, 0)
      }
    }

    if (chars.isEmpty) true
    else reduce(0, chars.length) == 0

}

  // For those who want more:
  // Prove that your reduction operator is associative!

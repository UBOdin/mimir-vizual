package org.mimirdb.vizual

import org.mimirdb.spark.expressionLogic.attributesOfExpression

import org.mimirdb.vizual.types._
import org.mimirdb.util.MergeMaps

/**
 * Sort update commands to present users with more intuitive, spreadsheet-like
 * behavior.
 * 
 * Vizual scripts are built in insertion order.  You make a change, and it gets
 * appended to the end of the script.  By contrast, Spreadsheets execute cells
 * in dependency order.  Consider the following operations on a given row:
 *   A = 3
 *   C = A + B
 *   B = 2
 * At the end of the script, C is going to be 5 (=3+2), in spite of the fact that 
 * it comes before B is set to 2.  To mimic this behavior, this function object
 * sorts a sequence of commands in topological order of their dependencies.  
 * Evaluating the resulting script in order produces the desired effect.
 * 
 * The algorithm is something along the lines of insertion sort with a bit of
 * bubble sort in for flavor.
 *
 * O(N^2)... how could you, Oliver?  Yeah, yeah, I know.
 *
 * Two key features here:
 *  1. We're dealing with a hopefully sparse *partial* order, so
 *     there's a good chance that the list is already sorted, and 
 *     we get insertion sort's best case O(N) performance.
 *  2. The conflict test isn't local... it needs to consider all 
 *     preceding elements of the list.  That means we're going to
 *     pay the N^2 cost in the worst case regardless.  With insertion
 *     sort we can still get O(N) performance if the list is already
 *     sorted.
 */
object MakeUpdatesLazy
{

  type Conflicts = Map[String, Seq[RowSelection]]

  class CyclicalFormulaDependency(a: Update, b: Update, conflicts: Conflicts)
    extends Exception(s"{ $conflicts } :: $a :: $b involves a cycle")

  def apply(commands: Seq[Update]): Seq[Update] =
    topologicalSort(commands.toList, Map.empty)._2


  /** 
   * A bubble-sort-esque recursive topological sort.
   * 
   * `commands` is a list of commands in arbitrary order.
   * `conflicts` is a collection of column/region pairs indexed by column.
   *
   * findNextCommute returns a 2-tuple (head, rest) with two guarantees.
   * - First, `update :: rest` is guaranteed to be a topologically sorted 
   *   transpose of `commands`. 
   * - Furthermore, `update` is an element of `commands` that writes to a column in
   *   `conflicts` with an overlapping region.  In other words, 
   * 
   * The basic trick is that findNextCommute recursively calls itself, each time
   * picking off the head of the list, checking if it conflicts with any preceding
   * elements (using `conflicts`), and then repeatedly pulling conflicting elements
   * back through the list.
   */ 
  def topologicalSort(commands: List[Update], conflicts: Conflicts):
    (Option[Update], List[Update]) =
  {
    commands match { 
      case Nil => return (None, Nil)
      case initialHead :: initialRest => {
        var head = initialHead
        var rest = initialRest

        // If all goes well, we should return from this loop well before
        // `rest.size` iterations (we should commute with each element of
        // `rest` at most once).  However, since condition-free loops are
        // likely to get us into an infinite loop one of these days, let's
        // cap the number of iterations just to be safe.
        for(i <- 0 until (rest.size+2)) {
          assert(rest.size <= initialRest.size)

          if(isAConflict(conflicts, head)){ 
            // Possibility 1: `head` conflicts with a preceding element in the
            // list.  Commute it backwards
            return (Some(head), rest)
          } else { 
            // Possibility 2: `head` is safe to follow preceding operations.
            // If so, find the first subsequent update that needs to be
            // commuted with it or another preceding operation.
            val (updateToCommute, remainingCommands) =
              topologicalSort(rest, extendConflicts(conflicts, head))

            if(updateToCommute.isEmpty) { 
              // If nothing needs to be commuted, we're done!
              return (None, head :: remainingCommands)
            } else {
              // for now, updates commute toll-free
              val (commutedUpdate, commutedHead) = (updateToCommute.get, Some(head))

              // if commuting update through head eliminates one of the two, then
              // pop the stack and repeat.
              if(commutedHead.isEmpty){
                rest = remainingCommands
                head = commutedUpdate
              } else { 
                // Test for cycles:
                if(isAConflict(extendConflicts(conflicts, commutedHead.get), commutedUpdate)) {
                  throw new CyclicalFormulaDependency( commutedUpdate, commutedHead.get, conflicts )
                }
                // If we're safe to swap, then swap and continue
                assert(remainingCommands.size <= rest.size - 1)
                rest = commutedHead.get :: remainingCommands
                head = commutedUpdate
              }
            }
          }
        }
        throw new Exception("Something went horribly wrong... we should have exited the topological sort by now.")
      } 
    }
  }

  /**
   * Test whether `command` conflicts with any of the listed conflicts
   * 
   * A command conflicts with a preceding command if it writes to a value
   * read from by the preceding command.  `conflicts` is expected to contain
   * a list of column/region pairs (indexed by column) of reads from preceding
   * operations.  `command` conflicts if 
   * 1. it writes to a column listed in `conflicts`
   * 2. its target region overlaps with the corresponding region of `conflicts`
   *
   * At present, this method has one shortcoming: Testing for overlap between
   * ExplicitRowSelection and PositionalRowSelection is currently not feasible.
   * see org.mimirdb.vizual.RowSelection for more details.
   *
   * Also note that column names are expected to be given *in lower case*
   */
  def isAConflict(conflicts: Conflicts, command: Update): Boolean = 
    command match {
      case Update(column, rows, value) => {
        if(conflicts contains column){
          conflicts(column).exists { _.canIntersect(rows) }
        } else { false }
      }
    }

  /**
   * Extend a list of conflicts to include the updated fields of a new
   * update command
   */
  def extendConflicts(initial: Conflicts, command: Update): Conflicts = 
  {
    val newConflicts = 
      attributesOfExpression(command.value.expr)
        .map { _.name.toLowerCase }
        .map { _ -> Seq(command.rows) }
        .toMap:Conflicts
    MergeMaps.simple(initial, newConflicts) { _ ++ _ }
  }


}
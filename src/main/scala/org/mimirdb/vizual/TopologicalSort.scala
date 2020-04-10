package org.mimirdb.vizual

import org.mimirdb.spark.expressionLogic.attributesOfExpression

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
 * sorts a sequence of updates in topological order of their dependencies.  
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
object TopologicalSort
{

  type Conflicts = Map[String, Seq[RowSelection]]

  class CyclicalFormulaDependency(a: Update, b: Update, conflicts: Conflicts)
    extends Exception(s"{ $conflicts } :: $a :: $b involves a cycle")

  def apply(updates: Seq[Update]): Seq[Update] =
    fetchNextCommute(updates.toList, Map.empty)._2


  /** 
   * A bubble-sort-esque recursive topological sort.
   * 
   * `updates` is a list of updates in arbitrary order.
   * `conflicts` is a collection of column/region pairs indexed by column.
   *
   * findNextCommute returns a 2-tuple (head, rest) with two guarantees.
   * - First, `update :: rest` is guaranteed to be a topologically sorted 
   *   transpose of `updates`. 
   * - Furthermore, `update` is an element of `updates` that writes to a column in
   *   `conflicts` with an overlapping region.  In other words, 
   * 
   * The basic trick is that findNextCommute recursively calls itself, each time
   * picking off the head of the list, checking if it conflicts with any preceding
   * elements (using `conflicts`), and then repeatedly pulling conflicting elements
   * back through the list.
   */ 
  def fetchNextCommute(updates: List[Update], conflicts: Conflicts):
    (Option[Update], List[Update]) =
  {
    updates match { 
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

          if(isAConflict(head, conflicts)){ 
            // Possibility 1: `head` conflicts with a preceding element in the
            // list.  Commute it backwards
            return (Some(head), rest)
          } else { 
            // Possibility 2: `head` is safe to follow preceding operations.
            // If so, find the first subsequent update that needs to be
            // commuted with it or another preceding operation.
            val (updateToCommute, remainingUpdates) =
              fetchNextCommute(rest, extendConflicts(head, conflicts))

            if(updateToCommute.isEmpty) { 
              // If nothing needs to be commuted, we're done!
              return (None, head :: remainingUpdates)
            } else {
              val update = updateToCommute.get
              // Test for cycles:
              if(isAConflict(head, extendConflicts(update, conflicts))){
                throw new CyclicalFormulaDependency( head, update, conflicts )
              }
              // If we're safe to swap, then swap and continue
              assert(remainingUpdates.size <= rest.size - 1)
              rest = head :: remainingUpdates
              head = update
            }
          }
        }
        throw new Exception("Something went horribly wrong... we should have exited the topological sort by now.")
      } 
    }
  }

  /**
   * Test whether `update` conflicts with any of the listed conflicts
   * 
   * An update conflicts with a preceding update if it writes to a value
   * read from by the preceding update.  `conflicts` is expected to contain
   * a list of column/region pairs (indexed by column) of reads from preceding
   * operations.  `update` conflicts if 
   * 1. it writes to a column listed in `conflicts`
   * 2. its target region overlaps with the corresponding region of `conflicts`
   *
   * At present, this method has one shortcoming: Testing for overlap between
   * ExplicitRowSelection and PositionalRowSelection is currently not feasible.
   * see org.mimirdb.vizual.RowSelection for more details.
   *
   * Also note that column names are expected to be given *in lower case*
   */
  def isAConflict(update: Update, conflicts: Conflicts): Boolean = 
    return conflicts.getOrElse(update.column.toLowerCase, Seq.empty)
                    .exists { update.rows.canIntersect(_) }


  /**
   * Extend a list of conflicts to include the updated fields of a new
   * update command
   */
  def extendConflicts(update: Update, initial: Conflicts): Conflicts = 
    attributesOfExpression(update.value.expr)
      .map { _.name.toLowerCase }
      .foldLeft(initial) { (curr, column) =>
        val originalConflictsForColumn = initial.getOrElse(column, Seq())
        val updatedConflictsForColumn = originalConflictsForColumn :+ update.rows
        initial ++ Some(column -> updatedConflictsForColumn) 
      }

}
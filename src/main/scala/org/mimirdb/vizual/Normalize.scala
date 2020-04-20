package org.mimirdb.vizual

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Attribute

class NormalizedScript(
  var prefix: Seq[PrefixCommand],
  var insertions: Seq[InsertRow],
  var deletions: Seq[DeleteRows],
  var updates: Seq[Update],
  var deletedCols: Seq[DeleteColumn]
){
  def commands: Seq[Command] =
    prefix ++ insertions ++ deletions ++ updates ++ deletedCols

  def replacePrefix(x: Seq[PrefixCommand])
    { prefix = x }

  def replaceInsertions(x: Seq[InsertRow])
    { insertions = x }

  def replaceDeletions(x: Seq[DeleteRows]) = 
    { deletions = x }

  def replaceUpdates(x: Seq[Update]) = 
    { updates = x }

  def replaceDeletedCols(x: Seq[DeleteColumn]) = 
    { deletedCols  = x }

  def transformPrefix(fn: Seq[PrefixCommand] => Seq[PrefixCommand])
    { replacePrefix(fn(prefix)) }

  def transformInsertions(fn: Seq[InsertRow] => Seq[InsertRow])
    { replaceInsertions(fn(insertions)) }

  def transformDeletions(fn: Seq[DeleteRows] => Seq[DeleteRows]) = 
    { replaceDeletions(fn(deletions)) }

  def transformUpdates(fn: Seq[Update] => Seq[Update]) = 
    { replaceUpdates(fn(updates)) }

  def transformDeletedCols(fn: Seq[DeleteColumn] => Seq[DeleteColumn]) = 
    { replaceDeletedCols(fn(deletedCols)) }

}

/**
 * Translate the script into a normal form
 * 
 * This means:
 *  - Ensure that positional cell identifiers are tagged.  This means creating all necessary tag 
 *    commands and removing None from the tag fields.
 *  - Ensure that Deleted column names are never re-used.  If necessary, DeleteColumn commands are
 *    rewritten into a rename + delete.
 *
 * The final result has the following structure
 * 1. Everything Else
 * 2. All InsertRows
 * 3. All Updates
 * 4. All DeleteColumns
 */
object Normalize {

  def apply(script: Seq[Command]): NormalizedScript =
  {
    var current = script

    current = TagPositionalReferences(current)
    current = AvoidReusingDeletedColumns(current)
    current = AssignInsertRowIdentifiers(current)

    val (ret1, deletedCols) = PushForwardDeleteColumn(current)
    current = ret1

    val (ret2, updates) = PushForwardUpdates(current)
    current = ret2

    val (ret3, deletions) = PushForwardDeleteRow(current)
    current = ret3

    val (ret4, insertions) = PushForwardInsertRow(current)
    current = ret4

    new NormalizedScript(
      AssertPrefix(current),
      insertions,
      deletions,
      updates,
      deletedCols ++ DeleteTagColumns(current)
    )
  }

  /**
   * Ensure that positional references are tied to tags, creating tag columns if necessary.
   */
  def TagPositionalReferences(script: Seq[Command]): Seq[Command] =
  {
    var currentTagId = 0
    var currentTag: Option[String] = None

    def makeTag: (Option[Command], String) =
    {
      currentTag match {
        case Some(tag) => (None, tag)
        case None => {
          currentTagId += 1
          val tag = s"__MIMIR_ORDER_TAG_${currentTagId}"
          currentTag = Some(tag)
          (Some(TagRowOrder(tag)), tag)
        }
      }
    }
    def wrapTag(build: (Option[String] => Command)): Seq[Command] =
    {
      val (command, tag) = makeTag
      command.toSeq :+ build(Some(tag))
    }

    // References to tags other than the current tag have been invalidated by *some* prior command.
    // In general, we shouldn't be seing ANY tags coming in to the normalize command... but just
    // in case something changes in the future, check for this potential error and fail-fast.
    def validateTag(tag: String) =
      if(currentTag == None || !tag.equals(currentTag.get)){
        throw new Exception(s"Reference to an invalid tag: '$tag'")
      }

    script.flatMap { 
      case x:DeleteColumn => 
        Seq(x)

      case x@DeleteRows(range) => 
        range match {
          case PositionRange(low, high, None) => {
            wrapTag { tag => DeleteRows(PositionRange(low, high, tag))}
          }
          case PositionRange(low, high, Some(tag)) => {
            validateTag(tag)
            Seq(x)
          }
          case _:GlobalRowIdentifiers => {
            // This invalidates the current tag
            currentTag = None
            Seq(x)
          }
          case AllRows() => 
            // Technically, since all rows are deleted, the current tag is... correct
            Seq(x)
        }

      case x:InsertColumn =>
        Seq(x)

      case InsertRow(position, None, identifier) => 
        wrapTag { tag => InsertRow(position, tag, identifier) }

      case x@InsertRow(_, Some(tag), _) => 
        validateTag(tag); Seq(x)

      case x:MoveColumn =>
        Seq(x)

      case x:RenameColumn =>
        Seq(x)

      case x:Sort =>
        currentTag = None; Seq(x)
      case x@Update(column, rows, value) => 
        rows match { 
          case PositionRange(low, high, None) => 
            wrapTag { tag => Update(column, PositionRange(low, high, tag), value) }
          case PositionRange(low, high, Some(tag)) =>
            validateTag(tag); Seq(x)
          case AllRows() => 
            Seq(x)
          case _:GlobalRowIdentifiers =>
            currentTag = None; Seq(x)
        }

      case x@TagRowOrder(tag) => 
        currentTag = Some(tag); Seq(x)

    }
  }

  /**
   * Ensure that deleted column names are never re-used, using rename if needed
   */
  def AvoidReusingDeletedColumns(script: Seq[Command]): Seq[Command] =
  {
    var currentId = 0
    def getFreshName: String = {
      currentId += 1
      s"__MIMIR_DELETED_COLUMN_${currentId}"
    }

    def targetsColumn(command:Command, column:String): Boolean =
    {
      command match {
        // As long as the column is never re-inserted, we should be fine
        case InsertColumn(x, _) => x.equalsIgnoreCase(column)
        case _ => false
      }
    }

    script.zipWithIndex.flatMap { 
      case (x@DeleteColumn(column), idx) => 
        if(script.slice(idx, script.size).exists { targetsColumn(_, column) }){
          val target = getFreshName
          Seq(
            RenameColumn(column, target),
            DeleteColumn(target)
          )
        } else { Seq(x) }
      case (x, _) => Seq(x)
    }
  }

  /**
   * Assign in-order identifiers to the InsertRow values
   */
  def AssignInsertRowIdentifiers(script: Seq[Command]): Seq[Command] =
  {
    var currentId = -1l
    script.map { 
      case InsertRow(position, tag, _) => currentId += 1; InsertRow(position, tag, Some(currentId))
      case x => x
    }
  }

  /**
   * Commute DeleteColumn commands forward
   */
  def PushForwardDeleteColumn(script: Seq[Command]): (Seq[Command], Seq[DeleteColumn]) =
  {
    // assuming that we ran AvoidReusingDeletedColumns before, this is a simple partition
    (
      script.filter { !_.isInstanceOf[DeleteColumn] },
      script.collect { case d:DeleteColumn => d }
    )
  }

  /**
   * Commute Update commands forward
   */
  def PushForwardUpdates(script: Seq[Command]): (Seq[Command], Seq[Update]) =
  {
    // We'll do the actual push forward momentarily.  First, we define some utility methods.
    // The real workhorse is here: commute swaps the order of a specified update and 
    def commute(update: Update, command: Command): (Seq[Command], Option[Update]) =
    {
      command match {
        case del:DeleteColumn => 
          // By the time we get here, all deletes should be pushed forward already.  Technically,
          // we can allow some to go through, but keep things simple for now.
          throw new Exception("Invalid Commute")
        case del:DeleteRows =>
          (del.rows, update.rows) match {
            // Don't bother updating if we just deleted all our rows.
            case (AllRows(), _)                 => Seq(del) -> None
            // Positional updates can change tag labels.  To commute, check if the tags overlap
            // and update the local range accordingly
            case (PositionRange(delLow, delHighMaybe, delTag),
                  PositionRange(updLow, updHighMaybe, updTag)) 
                    if delTag.get.equalsIgnoreCase(updTag.get) => 
            {
              def withRange(low: Long, high: Option[Long]) =
                Update(
                  update.column, 
                  PositionRange(low, high, updTag),
                  update.value
                )

              (delHighMaybe, updHighMaybe) match {
                // Possibility 1: Update << Delete
                // |<---- Update ---->|
                //                           |<--- Delete --->|
                // No need to modify anything
                case (_, Some(updHigh)) if updHigh <= delLow => 
                  Seq(del) -> Some(update)

                // Possibility 2: Delete << Update
                //                         |<---- Update ---->|
                // |<--- Delete --->|
                // Subtract (delHigh - delLow) from updLow and updHigh
                case (Some(delHigh), _) if delHigh <= updLow =>
                  {
                    val delta = delHigh - delLow
                    Seq(del) -> Some(withRange(updLow - delta, updHighMaybe.map { _ - delta }))
                  }

                // Possibility 3: Delete )= Update
                //       |<---- Update ---->|
                // |<---------- Delete ----------->|
                // Delete kills update
                case (None, _) if delLow <= updLow => 
                  Seq(del) -> None 
                case (Some(delHigh), Some(updHigh)) if delLow <= updLow && delHigh >= updHigh =>
                  Seq(del) -> None 

                // Possibility 4: Update )= Delete
                // |<---------- Update ----------->|
                //       |<---- Delete ---->|
                // Update High Shrinks by delHigh - delLow
                case (_, None) if updLow <= delLow => 
                  Seq(del) -> Some(update) // updHigh = infinity
                case (Some(delHigh), Some(updHigh)) if updLow <= delLow && updHigh >= delHigh =>
                  {
                    val delta = delHigh - delLow
                    Seq(del) -> Some(withRange(updLow, Some(updHigh - delta)))
                  }

                // Possibility 5: Partial intersection w/ Update on the left
                // |<----- Update ----->|
                //       |<---- Delete ---->|
                // update.high := delete.low
                case (_, Some(updHigh)) => 
                  Seq(del) -> Some(withRange(updLow, Some(delLow)))

                // Possibility 5: Partial intersection w/ Update on the right
                //       |<----- Update ----->|
                // |<---- Delete ---->|
                // update.low := delete.high
                case (Some(delHigh), _) => 
                  Seq(del) -> Some(withRange(delHigh, updHighMaybe))

                // Invalid case just to fix the sanity check... 
                // Should be covered by [Possibility 3] U [Possibility 4]
                case (None, None) => assert(false); null


              }
            }

            // All other update ranges should commute with a DeleteRow without issue
            case (_, _) => Seq(del) -> Some(update)

          } /* END match DeleteRows range */

        case _:InsertColumn => Seq(command) -> Some(update)
          

        // Row insertions are a bit of an odd case.  We tinker with the semantics of the default
        // values of the row to make this easier (and end up with more intuitive semantics in
        // the process).  Basically, rows inserted into a range being updated get their default
        // value from the update.  That means all we need to do is resize the update to account
        // for the tag manipulation being done by the row insertion operation.

        // Appending to the end requires no modifications to the update
        case InsertRow(None, _, _) => Seq(command) -> Some(update)

        case InsertRow(Some(pos), insTag, _) => 
          {
            update.rows match {
              // If the tags match, InsertRow adds 1 to all tags above `pos`.  Extend the range
              // of the update to cover the same set of rows (plus the inserted one).
              case PositionRange(low, high, updTag) 
                if updTag.get.equalsIgnoreCase(insTag.get) =>
              {
                def incementIfGreater(v:Long) = if (v >= pos) { v + 1 } else { v }
                Seq(command) -> Some(Update(
                  update.column,
                  PositionRange(incementIfGreater(low), high.map { incementIfGreater(_) }, updTag),
                  update.value
                ))
              }
              // If the tags don't match, or we're using another addressing scheme, then the
              // update isn't affected.
              case _ => Seq(command) -> Some(update)
            }
          }

        case _:MoveColumn => Seq(command) -> Some(update)

        case r:RenameColumn =>
          Seq(command) -> Some(Update(
            r.rename(update.column),
            update.rows,
            new Column(update.value.expr.transform { 
              case a:Attribute => r.rename(a)
            })
          ))

        // Even if positional indexing is being used, we expect all updates to be tagged by this
        // point, so Sort commutes without issue.
        case Sort(column, asc, virtualUpdates) => 
          Seq(Sort(column, asc, update +: virtualUpdates)) -> Some(update)

        // If we encounter an update, it's a bug, since all the updates should be getting 
        // commuted out of the remaining commands.
        case _:Update => throw new Exception("Not expecting to encounter an update")

        case _:TagRowOrder => Seq(command) -> Some(update)
      }
    }

    def commuteThrough(update: Update, commands: Seq[Command]): (Seq[Command], Option[Update]) = 
    {
      var curr:Option[Update] = Some(update)
      val commutedCommands = 
        commands.flatMap { command =>
          curr match {
            case None => Seq(command)
            case Some(u) => {
              val (commutedCommand, commutedUpdate) = commute(u, command)
              curr = commutedUpdate
              commutedCommand
            }
          }
        }
      (commutedCommands, curr)
    }

    script.foldRight( (Seq[Command](), Seq[Update]()) ) {
      case (next, (commands, updates)) =>
        next match {
          case update: Update => {
            val (commutedCommands, commutedUpdate) = commuteThrough(update, commands)
            (commutedCommands, commutedUpdate ++: updates)
          }
          case _ => (next +: commands, updates)
        }
    }
  }

  /**
   * Commute DeleteRow forward
   */
  def PushForwardDeleteRow(script: Seq[Command]): (Seq[Command], Seq[DeleteRows]) =
  {
    def commute(delete: DeleteRows, command: Command): (Seq[Command], Option[DeleteRows]) =
    {
      command match {
        case _:DeleteColumn | _:DeleteRows | _:Update => 
          // By the time we get here, all deletes should be pushed forward already.  Technically,
          // we can allow some to go through, but keep things simple for now.
          throw new Exception("Invalid Commute")

        case _:InsertColumn => Seq(command) -> Option(delete)

        case InsertRow(Some(position), insTag, identifier) => 
          delete.rows match {
            case PositionRange(low, highMaybe, delTag) if delTag.get.equalsIgnoreCase(insTag.get) =>
              if(position >= low) {
                if(highMaybe == None){
                  // The delete deletes all rows, and the insert places the new row into the
                  // range of deleted records.
                  // After the records are deleted, the inserted record would be placed at 
                  // the first deleted.  Increment the delete range to avoid deleting the newly
                  // inserted record.
                  Seq(InsertRow(
                    Some(low),
                    insTag,
                    identifier
                  )) -> Option(DeleteRows(
                    PositionRange(low+1, highMaybe, delTag)
                  ))
                } else {
                  // The inserted row comes after the range of deleted rows.  
                  // The delete range is unaffected, but the insert needs to be offset.
                  Seq(InsertRow(
                    Some(position + (highMaybe.get - low)),
                    insTag,
                    identifier
                  )) -> Option(delete)
                }
              } else {
                // the inserted row comes before the range of deleted rows.  
                // The insert position is unaffected, but the delete range needs to be offset
                Seq(command) -> Option(DeleteRows(
                  PositionRange(low+1, highMaybe.map { _ + 1 }, delTag)
                ))
              }
            // Otherwise, the operation passes through unchanged.
            case _ => Seq(command) -> Option(delete)
          }

        case InsertRow(None, insTag, identifier) => 
          delete.rows match {
            case PositionRange(low, highMaybe, delTag) if delTag.get.equalsIgnoreCase(insTag.get) =>
              if(highMaybe == None){
                // if we truncate the table, appending the row to the end of the post-delete structure
                // is equivalent to placing the row at position `low` and offsetting the delete range 
                // by 1.
                Seq(InsertRow(
                  Some(low),
                  insTag,
                  identifier
                )) -> Option(DeleteRows(
                  PositionRange(low+1, None, delTag)
                ))
              } else {
                // otherwise, append and then delete shouldn't interfere
                Seq(command) -> Option(delete)
              }
            case _ => Seq(command) -> Option(delete)
          }

        case _:MoveColumn => Seq(command) -> Option(delete)
        case _:RenameColumn => Seq(command) -> Option(delete)
        case _:Sort => Seq(command) -> Option(delete)
        case _:TagRowOrder => Seq(command) -> Option(delete)
      }
    }

    def commuteThrough(delete: DeleteRows, commands: Seq[Command]): (Seq[Command], Option[DeleteRows]) = 
    {
      var curr:Option[DeleteRows] = Some(delete)
      val commutedCommands = 
        commands.flatMap { command =>
          curr match {
            case None => Seq(command)
            case Some(x) => {
              val (commutedCommand, commutedDelete) = commute(x, command)
              curr = commutedDelete.headOption
              commutedCommand
            }
          }
        }
      (commutedCommands, curr)
    }

    script.foldRight( (Seq[Command](), Seq[DeleteRows]()) ) {
      case (next, (commands, deletes)) =>
        next match {
          case delete: DeleteRows => {
            val (commutedCommands, commutedDelete) = commuteThrough(delete, commands)
            (commutedCommands, commutedDelete ++: deletes)
          }
          case _ => (next +: commands, deletes)
        }
    }
  }

  /**
   * Commute Insertions forward
   */
  def PushForwardInsertRow(script: Seq[Command]): (Seq[Command], Seq[InsertRow]) =
  {
    def commute(insert: InsertRow, command: Command): (Seq[Command], Option[InsertRow]) =
    {
      command match {
        case _:DeleteColumn | _:DeleteRows | _:Update | _:InsertRow => 
          // By the time we get here, all deletes should be pushed forward already.  Technically,
          // we can allow some to go through, but keep things simple for now.
          throw new Exception(s"Invalid Commute: $command")

        case _:InsertColumn => Seq(command) -> Option(insert)
        case _:MoveColumn => Seq(command) -> Option(insert)
        case _:RenameColumn => Seq(command) -> Option(insert)
        case _:Sort => Seq(command) -> Option(insert)
        case _:TagRowOrder => Seq(command) -> Option(insert)

      }
    }

    def commuteThrough(insert: InsertRow, commands: Seq[Command]): (Seq[Command], Option[InsertRow]) = 
    {
      var curr:Option[InsertRow] = Some(insert)
      val commutedCommands = 
        commands.flatMap { command =>
          curr match {
            case None => Seq(command)
            case Some(x) => {
              val (commutedCommand, commutedInsert) = commute(x, command)
              curr = commutedInsert
              commutedCommand
            }
          }
        }
      (commutedCommands, curr)
    }

    script.foldRight( (Seq[Command](), Seq[InsertRow]()) ) {
      case (next, (commands, inserts)) =>
        next match {
          case insert: InsertRow => {
            val (commutedCommands, commutedInsert) = commuteThrough(insert, commands)
            (commutedCommands, commutedInsert ++: inserts)
          }
          case _ => (next +: commands, inserts)
        }
    }
  }

  def AssertPrefix(script: Seq[Command]): Seq[PrefixCommand] =
  {
    script.map { c => 
      assert(c.isInstanceOf[PrefixCommand])
      c.asInstanceOf[PrefixCommand]
    }
  }

  def DeleteTagColumns(script: Seq[Command]): Seq[DeleteColumn] =
  {
    script.collect { 
      case TagRowOrder(column) => DeleteColumn(column)
    }
  }
}
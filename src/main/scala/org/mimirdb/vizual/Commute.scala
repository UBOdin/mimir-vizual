package org.mimirdb.vizual

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.Attribute


/** 
 * Helpers for commuting operators forward in Normalize 
 * 
 * Note: These do not implement general commutativity rules for Vizual.  They are implemented with
 * a range of assumptions that may not hold outside of Normalize().  Use elsewhere with care.
 */
object Commute
{
  def through(update: Update, commands: Seq[Command]): (Seq[Command], Option[Update]) = 
  {
    var curr:Option[Update] = Some(update)
    val commutedCommands = 
      commands.flatMap { command =>
        curr match {
          case None => Seq(command)
          case Some(u) => {
            val (commutedCommand, commutedUpdate) = apply(u, command)
            curr = commutedUpdate
            commutedCommand
          }
        }
      }
    (commutedCommands, curr)
  }


  def through(delete: DeleteRows, commands: Seq[Command]): (Seq[Command], Option[DeleteRows]) = 
  {
    var curr:Option[DeleteRows] = Some(delete)
    val commutedCommands = 
      commands.flatMap { command =>
        curr match {
          case None => Seq(command)
          case Some(x) => {
            val (commutedCommand, commutedDelete) = apply(x, command)
            curr = commutedDelete.headOption
            commutedCommand
          }
        }
      }
    (commutedCommands, curr)
  }

  def through(insert: InsertRow, commands: Seq[Command]): (Seq[Command], Option[InsertRow]) = 
  {
    var curr:Option[InsertRow] = Some(insert)
    val commutedCommands = 
      commands.flatMap { command =>
        curr match {
          case None => Seq(command)
          case Some(x) => {
            val (commutedCommand, commutedInsert) = apply(x, command)
            curr = commutedInsert
            commutedCommand
          }
        }
      }
    (commutedCommands, curr)
  }

  def apply(update: Update, command: Command): (Seq[Command], Option[Update]) =
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
      case Sort(column, asc) => 
        Seq(Sort(column, asc)) -> Some(update)

      // If we encounter an update, it's a bug, since all the updates should be getting 
      // commuted out of the remaining commands.
      case _:Update => throw new Exception("Not expecting to encounter an update")

      case _:TagRowOrder => Seq(command) -> Some(update)
    }
  }

  def apply(delete: DeleteRows, command: Command): (Seq[Command], Option[DeleteRows]) =
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

  def apply(insert: InsertRow, command: Command): (Seq[Command], Option[InsertRow]) =
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
  
}
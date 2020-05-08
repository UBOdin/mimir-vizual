package org.mimirdb.vizual

import org.apache.spark.sql.types.StructType

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

  def apply(script: Seq[Command], schema: StructType): CompiledScript =
  {
    var current = script

    def extract[A](v:(Seq[Command], A)):A = { current = v._1; v._2 }

    current = TagPositionalReferences(current)
    current = AvoidReusingDeletedColumns(current)
    current = AssignInsertRowIdentifiers(current)

    val outputs     = extract { ComputeFinalSchema(current, schema.fieldNames) }
    val sort        = extract { PushForwardSort(current) }
    val updates     = extract { PushForwardUpdates(current) }
    val deletions   = extract { PushForwardDeleteRow(current) }
    val insertions  = extract { PushForwardInsertsAndTags(current) }
    val preprocess  = current.collect { case x:PreprocessCommand => x }
    assert(preprocess.size == current.size)
    val inputs      = ComputeInputSchema(preprocess, schema.fieldNames)

    new CompiledScript(
      inputs,
      insertions,
      deletions,
      MakeUpdatesLazy(updates),
      sort,
      outputs
    )
  }

  /**
   * Ensure that positional references are tied to tags, creating tag columns if necessary.
   */
  def TagPositionalReferences(script: Seq[Command]): Seq[Command] =
  {
    var currentTagId = 0
    var currentTag: Option[String] = None

    def makeTag(idx: Int): (Option[Command], String) =
    {
      currentTag match {
        case Some(tag) => (None, tag)
        case None => {
          currentTagId += 1
          val tag = s"__MIMIR_ORDER_TAG_${currentTagId}"
          currentTag = Some(tag)
          (Some(TagRowOrder(tag, script.slice(0, idx))), tag)
        }
      }
    }
    def wrapTag(idx: Int)(build: (Option[String] => Command)): Seq[Command] =
    {
      val (command, tag) = makeTag(idx)
      command.toSeq :+ build(Some(tag))
    }

    // References to tags other than the current tag have been invalidated by *some* prior command.
    // In general, we shouldn't be seing ANY tags coming in to the normalize command... but just
    // in case something changes in the future, check for this potential error and fail-fast.
    def validateTag(tag: String) =
      if(currentTag == None || !tag.equals(currentTag.get)){
        throw new Exception(s"Reference to an invalid tag: '$tag'")
      }

    script.zipWithIndex.flatMap { 
      case (x:DeleteColumn, _) => 
        Seq(x)

      case (x@DeleteRows(range), idx) => 
        range match {
          case PositionRange(low, high, None) => {
            wrapTag(idx) { tag => DeleteRows(PositionRange(low, high, tag))}
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

      case (x:InsertColumn, _) =>
        Seq(x)

      case (InsertRow(position, None, identifier), idx) => 
        wrapTag(idx) { tag => InsertRow(position, tag, identifier) }

      case (x@InsertRow(_, Some(tag), _), _) => 
        validateTag(tag); Seq(x)

      case (x:MoveColumn, _) =>
        Seq(x)

      case (x:RenameColumn, _) =>
        Seq(x)

      case (x:Sort, idx) =>
        // The last sort (if any) needs to be tagged
        if(script.slice(idx+1, script.length).exists { case _:Sort => true; case _ => false })
             { currentTag = None; Seq(x) }
        else { currentTag = None; val (tag, col) = makeTag(idx+1); Seq(x) ++ tag  }

      case (x@Update(column, rows, value), idx) => 
        rows match { 
          case PositionRange(low, high, None) => 
            wrapTag(idx) { tag => Update(column, PositionRange(low, high, tag), value) }
          case PositionRange(low, high, Some(tag)) =>
            validateTag(tag); Seq(x)
          case AllRows() => 
            Seq(x)
          case _:GlobalRowIdentifiers =>
            currentTag = None; Seq(x)
        }

      case (x@TagRowOrder(tag, _), _) => 
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

  def ComputeFinalSchema(script: Seq[Command], schema:Seq[String]): (Seq[Command], Seq[String]) =
  {

    def inject(col: String, sch: Seq[String], pos: Int) = 
      (sch.slice(0, pos) :+ col) ++ sch.slice(pos, sch.size)
    def drop(col: String, sch: Seq[String]) =
      sch.filter { !_.equalsIgnoreCase(col) }

    (
      script.filter { !_.isInstanceOf[PostprocessCommand] },
      script.foldLeft(schema) { 
        case (sch, InsertColumn(col, None)) => sch :+ col
        case (sch, InsertColumn(col, Some(pos))) => inject(col, sch, pos)
        case (sch, DeleteColumn(col)) => drop(col, sch)
        case (sch, r:RenameColumn) => sch.map { r.rename(_) }
        case (sch, MoveColumn(col, pos)) => inject(col, drop(col, sch), pos)
        case (sch, _:DeleteRows | _:InsertRow | _:Sort | _:Update | _:TagRowOrder) => sch
      }
    )
  }

  def ComputeInputSchema(script: Seq[PreprocessCommand], schema:Seq[String]): Seq[(String, Option[String])] =
  {
    script.foldLeft(schema.map { col => (col -> Some(col)):(String, Option[String]) }) {
      case (sch, InsertColumn(col, _)) => sch :+ (col -> None)
      case (sch, RenameColumn(col, other)) => {
        val renamed = sch.map { 
          case element if element._1.equalsIgnoreCase(col) => other -> element._2
          case element => element
        }
        assert(renamed.exists { _._1.equalsIgnoreCase(other) })
        renamed
      }
    }
  }

  /**
   * Commute DeleteColumn commands forward
   */
  def PushForwardPostprocess(script: Seq[Command]): (Seq[Command], Seq[PostprocessCommand]) =
  {
    script.foldRight( (Seq[Command](), Seq[PostprocessCommand]()) ) {
      case (next, (commands, post)) =>
        next match {
          case delete: DeleteColumn => (commands, delete +: post)
          case move: MoveColumn => (commands, move +: post)
          case _ => (next +: commands, post)
        }
    }
  }

  def PushForwardSort(script: Seq[Command]): (Seq[Command], Option[Sort]) =
  {
    // TagPositionalReferences ensures that the final sort command is tagged.  We can't use the
    // sort in place... but we can use the corresponding tag.  Filter out all the other sorts.
    (
      script.filter { case _:Sort => false; case _ => true },
      script.collect { case t:TagRowOrder => t }
            .reverse.headOption
            .map { t => Sort(t.column, true) }
    )
  }

  /**
   * Commute Update commands forward
   */
  def PushForwardUpdates(script: Seq[Command]): (Seq[Command], Seq[Update]) =
  {
    script.foldRight( (Seq[Command](), Seq[Update]()) ) {
      case (next, (commands, updates)) =>
        next match {
          case update: Update => {
            val (commutedCommands, commutedUpdate) = Commute.through(update, commands)
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
    script.foldRight( (Seq[Command](), Seq[DeleteRows]()) ) {
      case (next, (commands, deletes)) =>
        next match {
          case delete: DeleteRows => {
            val (commutedCommands, commutedDelete) = Commute.through(delete, commands)
            (commutedCommands, commutedDelete ++: deletes)
          }
          case _ => (next +: commands, deletes)
        }
    }
  }

  /**
   * Commute Insertions forward
   */
  def PushForwardInsertsAndTags(script: Seq[Command]): 
    (Seq[Command], (Seq[InsertRow], Seq[(TagRowOrder, Seq[InsertRow])])) =
  {
    val (ret, initialInserts, postTagInserts) =
      script.foldRight( (Seq[Command](), Seq[InsertRow](), Seq[(TagRowOrder, Seq[InsertRow])]()) ) {
        case (next, (commands, initialInserts, postTagInserts)) =>
          next match {
            case insert: InsertRow => {
              val (commutedCommands, commutedInsert) = Commute.through(insert, commands)
              (commutedCommands, commutedInsert ++: initialInserts, postTagInserts)
            }
            case tag: TagRowOrder => {
              // At this point, we should have only preprocessCommand instructions, which we can
              // safely commute through
              assert(!commands.exists { !_.isInstanceOf[PreprocessCommand] })
              (commands, Seq[InsertRow](), (tag, initialInserts) +: postTagInserts)
            }
            case _ => (next +: commands, initialInserts, postTagInserts)
          }
      }
    (ret, (initialInserts, postTagInserts))
  }
}
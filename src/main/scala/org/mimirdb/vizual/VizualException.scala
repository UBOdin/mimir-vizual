package org.mimirdb.vizual

class VizualException(message: String, command: Command) extends Exception(s"$message in $command")
package com.nicta.scoobi.impl.util

import org.apache.hadoop.fs.{FileSystem, Path, FileStatus}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io.SequenceFile.Reader
import java.net.URI
import org.apache.hadoop.filecache.DistributedCache

object Compatibility {

  /** @return true if the file is a directory */
  def isDirectory(fileStatus: FileStatus): Boolean =
    fileStatus.isDir

  /** @return a sequence file reader */
  def newSequenceFileReader(configuration: Configuration, path: Path): SequenceFile.Reader =
    new Reader(FileSystem.get(configuration), path, configuration)

  /**
   * create TaskAttemptContext from a JobConf and jobId 
   */
  def newTaskAttemptContext(conf: Configuration, id: TaskAttemptID): TaskAttemptContext =
    new TaskAttemptContext(conf, id)

  /**
   * create JobContext from a configuration and jobId
   */
  def newJobContext(conf: Configuration, id: JobID): JobContext =
    new JobContext(conf, id)

  /**
   * create a new Job from a configuration
   */
  def newJob(conf: Configuration): Job =
    new Job(conf)

  /**
   * create a new Job from a configuration and name
   */
  def newJob(conf: Configuration, name: String): Job =
    new Job(conf, name)

  /**
   * create a MapContext from a JobConf and jobId
   */
  def newMapContext(conf: Configuration, id: TaskAttemptID, reader: RecordReader[Any,Any], writer: RecordWriter[Any,Any], outputCommitter: OutputCommitter, reporter: StatusReporter, split: InputSplit): MapContext[Any,Any,Any,Any] =
    new MapContext(conf, id, reader, writer, outputCommitter, reporter, split)

  /**
   * Rename method using the FileSystem for cdh3 and FileContext (i.e. not broken when moving directories) for cdh4 and cdh5
   */
  def rename(srcPath: Path, destPath: Path)(implicit configuration: Configuration) =
    FileSystem.get(configuration).rename(srcPath, destPath)

  /**
   * Invokes Configuration() on JobContext. Works with both
   * hadoop 1 and 2.
   */
  def getConfiguration(context: JobContext): Configuration =
    context.getConfiguration

  lazy val defaultFSKeyName = "fs.default.name"

  lazy val cache = HadoopDistributedCache()

  case class HadoopDistributedCache() {
    lazy val CACHE_FILES = "mapred.cache.files"

    def addCacheFile(uri: URI, configuration: Configuration): Unit =
      DistributedCache.addCacheFile(uri, configuration)

    def getLocalCacheFiles(configuration: Configuration): Array[Path] =
      DistributedCache.getLocalCacheFiles(configuration)

    def getCacheFiles(configuration: Configuration): Array[URI] =
      DistributedCache.getCacheFiles(configuration)

    def createSymlink(configuration: Configuration): Unit =
      DistributedCache.createSymlink(configuration)

    def addFileToClassPath(path: Path, configuration: Configuration): Unit =
      DistributedCache.addFileToClassPath(path, configuration)
  }

}

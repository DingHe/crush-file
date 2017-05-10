package com.caishi.spark.crushfile

import org.apache.hadoop.fs._
import org.apache.parquet.filter2.predicate.Operators.Column
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

/**
 * 合并日志文件：目前支持parquet file
 * 1.从fromDir目录中加载数据文件
 * 2.将文件输出到tmpDir，文件已arh字符开头
 * 3.mv tmpDir中的文件到fromDir中
 * 4.删除fromDir目录中非arh开头的文件
 * Created by root on 15-10-28.
 */
object CrushFile {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: " +
        "| CrushFile <dfsUri> <abFromDir> <abTmpDir>")
      System.exit(1)
    }
    val Array(dfsUri,fromDir,tmpDir) = args
//    val dfsUri = "hdfs://192.168.100.73:9000"
//    val fromDir ="/logdata/2016/06/07/topic-user-active/00,/logdata/2016/06/07/topic-user-active/01, /logdata/2016/06/07/topic-user-active/02,/logdata/2016/06/07/topic-user-active/03, /logdata/2016/06/07/topic-user-active/04,/logdata/2016/06/07/topic-user-active/05, /logdata/2016/06/07/topic-user-active/06,/logdata/2016/06/07/topic-user-active/07, /logdata/2016/06/07/topic-user-active/08,/logdata/2016/06/07/topic-user-active/09, /logdata/2016/06/07/topic-user-active/10,/logdata/2016/06/07/topic-user-active/11"
//    val tmpDir = "/tmp/crush-file-tmp"
    // 223   109  56
    val sc = SparkContextSingleton.getInstance()
    sc.hadoopConfiguration.set("fs.defaultFS",dfsUri)
    val fs = FileSystem.get(sc.hadoopConfiguration)
    val sql = SQLContextSingleton.getInstance(sc)
//      sc.textFile("").map(_=>"").collect()
    val td= dfsUri+fromDir
    val df = sql.read.parquet(td)
//        df.repartition(takePartition(td, fs)).write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir)
    df.coalesce(1).write.format("parquet").mode(SaveMode.Overwrite).save(tmpDir)
    //  mv临时目录下的 新文件 到目标目录
    mv(dfsUri+tmpDir, td, fs)
    // 删除原始小文件
    del(td, fs)

  }

  /** 根据输入目录计算目录大小，并以128*2M大小计算partition */
  def takePartition(src : String,fs : FileSystem): Int ={
    val cos:ContentSummary = fs.getContentSummary(new Path(src))
    val sizeM:Long = cos.getLength/1024/1024
    val partNum:Int = sizeM/256 match {
      case 0 => 1
      case _ => (sizeM/256).toInt
    }
    partNum
  }

  /** 将临时目录中的结果文件mv到源目录，并以ahr-为文件前缀 */
  def mv (fromDir:String,toDir:String,fs:FileSystem): Unit ={
    val srcFiles : Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir)));
    for(p : Path <- srcFiles){
      // 如果是以part开头的文件则修改名称
      if(p.getName.startsWith("part")){
        fs.rename(p,new Path(toDir+"/ahr-"+p.getName))
      }
    }
  }

  /** 删除原始小文件 */
  def del(fromDir : String,fs : FileSystem): Unit ={
    val files : Array[Path] = FileUtil.stat2Paths(fs.listStatus(new Path(fromDir),new FileFilter()))
    for(f : Path <- files){
      fs.delete(f,true)// 迭代删除文件或目录
    }
  }
}


class FileFilter extends  PathFilter{
  @Override  def accept(path : Path) : Boolean = {
    !path.getName().startsWith("ahr-")
  }
}

/** Lazily instantiated singleton instance of SQLContext */
object SQLContextSingleton {
  @transient  private var instance: SQLContext = _
  def getInstance(): SQLContext = {
    if (instance == null) {
      var sc = SparkContextSingleton.getInstance()
      instance = new SQLContext(sc)
      instance.setConf("spark.sql.parquet.compression.codec", "snappy")
    }
    instance
  }

  def getInstance(sc : SparkContext): SQLContext = {
    if (instance == null) {
      instance = new SQLContext(sc)
      instance.setConf("spark.sql.parquet.compression.codec", "snappy")
    }
    instance
  }
}

object SparkContextSingleton {
  @transient  private var instance: SparkContext = _
  def getInstance(): SparkContext = {
    if (instance == null) {
      val sparkConf = new SparkConf().setAppName("spark-crushfile")
      instance = new SparkContext(sparkConf)
    }
    instance
  }
}

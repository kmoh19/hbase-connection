package com.dev.hbase_spark

import org.apache.hadoop.conf.Configuration

import org.apache.hadoop.hbase.util.HBaseConfTool

import org.apache.hadoop.hbase.HBaseConfiguration

import org.apache.hadoop.hbase.client.Connection

import org.apache.hadoop.hbase.client.ConnectionFactory

import org.apache.hadoop.hbase.TableName

import org.apache.hadoop.hbase.util.Bytes

import org.apache.hadoop.hbase.client.Put

import org.apache.hadoop.hbase.client.Get

import org.apache.hadoop.hbase.io.ImmutableBytesWritable

import org.apache.hadoop.hbase.mapreduce.TableInputFormat

import org.apache.hadoop.hbase.client.Result

import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.SQLContext

object mypipe {
  
  val conf: Configuration = HBaseConfiguration.create()
  
  def main(args: Array[String]):Unit = {
    
    import org.apache.spark.SparkContext
    
    val sc = new SparkContext("local", "hbase-test")
    
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    
    import sqlContext.implicits._

    conf.set("hbase.zookeeper.quorum", "localhost")

    conf.set("hbase.zookeeper.property.clientPort", "2181")
    

    val connection: Connection = ConnectionFactory.createConnection(conf)

    val table = connection.getTable(TableName.valueOf("employee"))

    print("connection created")
    
   

      for (rowKey <- 1 to 1) {
         
        println(table.get(new Get(Bytes.toBytes(rowKey.toString()))))

      val result = table.get(new Get(Bytes.toBytes(rowKey.toString())))

      val nameDetails = result.getValue(Bytes.toBytes("emp personal data"), Bytes.toBytes("name"))

      val cityDetails = result.getValue(Bytes.toBytes("emp personal data"), Bytes.toBytes("city"))

      val designationDetails = result.getValue(Bytes.toBytes("emp professional data"), Bytes.toBytes("designation"))

      val salaryDetails = result.getValue(Bytes.toBytes("emp professional data"), Bytes.toBytes("salary"))    

      val name = Bytes.toString(nameDetails)

      val city = Bytes.toString(cityDetails)

      val designation = Bytes.toString(designationDetails)

      val salary = Bytes.toString(salaryDetails)

      println("Name is.... " + name + ", city " + city + ", Designation " + designation + ", Salary " + salary)
      
     
      //loading new table
      conf.set(TableInputFormat.INPUT_TABLE, "employee")
      
      val hBaseRDD = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      
      val resultRDD = hBaseRDD.map(tuple => tuple._2)
      
      val strResultRDD= resultRDD.map(x=>Bytes.toString(x.getValue(Bytes.toBytes("emp personal data"),Bytes.toBytes("name"))))
      val resultDF=strResultRDD.toDF()
      
      ret(resultDF)
      val sstr= strResultRDD.collect()
      sstr.foreach(println)
    }
   def ret(myrdd:DataFrame) : DataFrame = {
  
       myrdd
}
   
  }
  
  
  
  
}
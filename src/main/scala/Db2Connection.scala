import org.apache.hadoop.fs.LocalFileSystem
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.spark.sql.{DataFrame, SparkSession}

object Db2Connection {
     val inputpath="cos://candidate-exercise.myCos/emp-data.csv"
     val outputpath="cos://candidate-exercise.myCos/emp-data-output.csv"
     val dburl = "jdbc:db2:Server=localhost;Port=50000;User=admin;Password=admin;Database=test"
     val sqlTable = "emp-data"


//1.	Function that connects to IBM Cloud Object Store (COS)
  def setHadoopConfigurationForIBMCOS(sparkSession: SparkSession, path: String) = {
    sparkSession.sparkContext.hadoopConfiguration.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.file.impl", classOf[LocalFileSystem].getName)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.servicename.endpoint", "s3.us.cloud-object-storage.appdomain.cloud")
   // sparkSession.sparkContext.hadoopConfiguration.set("fs.defaultFS", path)
    sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.servicename.secret.key","27b804de3b329a680dbf148fd76da208f33e8a5aaaea4cbd")
     sparkSession.sparkContext.hadoopConfiguration.set("fs.cos.servicename.access.key","0aba66146f3b450cacebaa908046d17e")
    sparkSession.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.stocator.scheme.list", "cos")
    sparkSession.sparkContext.hadoopConfiguration.set("spark.hadoop.fs.cos.impl", "com.ibm.stocator.fs.ObjectStoreFileSystem")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.stocator.cos.impl", "com.ibm.stocator.fs.cos.COSAPIClient")
    sparkSession.sparkContext.hadoopConfiguration.set("fs.stocator.cos.scheme", "cos")

  }
  //2.	Function that reads a CSV file (emp-data.csv) from the COS bucket
  def readFromCOS(sparkSession: SparkSession, path: String ): DataFrame = {
    import sparkSession.implicits._
    val readdf=sparkSession.read.csv(path)
    readdf.printSchema()
    readdf
}

  def db2Connection(sparkSession: SparkSession, databaseURL: String, dbtable: String): DataFrame = {
    val db2Connect = sparkSession.read.format("jdbc").option("url", dburl).option("dbtable", sqlTable).option("driver", "cdata.jdbc.db2.DB2Driver").load()
    println(db2Connect,"connection success")
    db2Connect

  }
  def filteredData(sparkSession: SparkSession, db2ConnectDF: DataFrame ,path:String): DataFrame = {
    /*
      CREATE TABLE empdata(ID int, Name varchar(4), Department varchar(4), Gender varchar(6) ,salary int);
  INSERT INTO empdata
   VALUES
       (10, 'Sam', 'IT', 'FEMALE',1000),
       (2, 'Jiya', 'FI', 'MALE',2000),
       (3, 'Mani', 'sale', 'MALE',3000),
     (5, 'Pree', 'FI', 'FEMALE',2000),
     (4, 'Chris', 'FI', 'FEMALE',4000); ;
    */
    import sparkSession.implicits._

//    //5.	Write Scala code to read the same data from the database, calculate and display the following:
    val ratioDepartmentDf=db2ConnectDF
    ratioDepartmentDf.registerTempTable("empdata")
//    //a.	Gender ratio in each department
      sparkSession.sql("select distinct(Department), Gender, count(*) over( partition by Department, Gender) cnt from empdata1 order by Department desc, Gender desc")
//   // b.	Average salary in each department
    val avgSalary_df=sparkSession.sql("select DEPARTMENT, sum(SALARY)/count(ID) as AVGSalary from empdata group by DEPARTMENT")
    //=avgSalary_df.
    //c.	Male and female salary gap in each department
    val avg_gap=sparkSession.sql("select m.department,coalesce(male_avg_slary-female_avg_slary,0) AS field_alias from (select department,sum(salary)/count(id) as male_avg_slary from empdata where gender='MALE' group by department) m full outer join (select department,sum(salary)/count(id) as female_avg_slary from empdata where gender='FEMALE' group by department) f on m.department=f.department;")
//    //6.	Scala code to write one the calculated data as a Parquet back to COS
    avgSalary_df.write.format("csv").save(path)//"cos://candidate-exercise.myCos/emp-data-output.csv
  avgSalary_df
  }
  def main(args: Array[String]): Unit = {
        val sparkSession = SparkSession
          .builder()
          .appName("IBMExample")
          .master("local[*]")
          .getOrCreate()
    db2Connection(sparkSession,dburl,sqlTable)
  val connectIBMCos=setHadoopConfigurationForIBMCOS(sparkSession,inputpath)
//    println(connectIBMCos)
    val readData=readFromCOS(sparkSession,"cos://candidate-exercise.myCos/emp-data.csv")
    val db2ConnectDF=db2Connection(sparkSession,dburl,sqlTable)
    val filteredDF = filteredData(sparkSession, db2ConnectDF,outputpath)

  }
  }

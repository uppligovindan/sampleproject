package pack



import org.apache.spark.SparkContext  
import org.apache.spark.sql.SparkSession  
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.functions.upper
import org.apache.spark.sql.catalyst.expressions.Upper
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import scala.io.Source
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._



object objpro {

	def main(args:Array[String]):Unit={


			val conf = new SparkConf().setAppName("ES").setMaster("local[*]")

					val sc = new SparkContext(conf)
					sc.setLogLevel("Error")


					val spark = SparkSession.builder()



					.getOrCreate()

					import spark.implicits._


			
					
		



					val data = spark.read.format("avro")
					.load("file:///C://data/project/projectsample.avro")

					data.show()



					
	


					val html = Source.fromURL("https://randomuser.me/api/0.8/?results=500")
					val s = html.mkString
			


					val urldf = spark.read.json(sc.parallelize(List(s)))
					urldf.show()


			
		
					val flatdf = urldf.withColumn("results",explode(col("results"))).select("nationality","seed","version",
							"results.user.username","results.user.cell","results.user.dob","results.user.email",
							"results.user.gender","results.user.location.city","results.user.location.state",
							"results.user.location.street","results.user.location.zip","results.user.md5",
							"results.user.name.first","results.user.name.last","results.user.name.title",
							"results.user.password","results.user.phone","results.user.picture.large","results.user.picture.medium"
							,"results.user.picture.thumbnail","results.user.registered","results.user.salt","results.user.sha1"
							,"results.user.sha256")
					flatdf.show()






			

					val rm=flatdf.withColumn("username",regexp_replace(col("username"),  "([0-9])", ""))
					rm.show()

	
					
	
					val joindf = data.join(broadcast(rm),Seq("username"),"left")


					joindf.show()


					
		


					val dfnull = joindf.filter(col("nationality").isNull)


					val dfnotnull=joindf.filter(col("nationality").isNotNull)



					dfnull.show()




					println
					
					
		
					println
					println("==================  Step 7 b =============available customers=============================================")
					println
					println
			




					dfnotnull.show()

				
				



					val replacenull= dfnull.na.fill("Not Available").na.fill(0)
					replacenull.show()


		
				
				



					val replacenull_with_current_date=replacenull.withColumn("current_date",current_date)

					replacenull_with_current_date.show()



				
					
	



					val notnull_with_current_date=dfnotnull.withColumn("current_date",current_date)


					notnull_with_current_date.show()




					notnull_with_current_date.write.format("parquet").mode("append").partitionBy("current_date")
					            .save("file:///C:/data/project/availablecustomer")
					
					
					
					replacenull_with_current_date.write.format("parquet").mode("append").partitionBy("current_date")
					            .save("file:///C:/data/project/notavailablecustomer")
					
					
					
					
					
					




	}

}
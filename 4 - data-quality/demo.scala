/* download deequ jar from https://search.maven.org/artifact/com.amazon.deequ/deequ/1.0.5/jar
   make sure you are on java 8: sudo update-alternatives --config java
   run:  
   cd ~/repos/ndc-sydney-2020/4 - data-quality
   ~/spark-2.4.7/bin/spark-shell --driver-memory 12g --jars ./deequ-1.0.5.jar
*/
import com.amazon.deequ.VerificationSuite
import com.amazon.deequ.checks.{Check, CheckLevel, CheckStatus}
import com.amazon.deequ.constraints.ConstraintStatus

def readCsv(path: String) = spark.read.option("header", "true").option("inferSchema", "true").csv(path)

val countries = readCsv("countries.csv")
countries.show(5)


val orders = readCsv("orders.csv")
orders.show(5)

val ordersWithCountryName = orders.join(countries, Seq("CountryCode")).drop("CountryCode")
ordersWithCountryName.show(5)

def createVerificationSuite(data: org.apache.spark.sql.DataFrame) = VerificationSuite()
  .onData(data)
  .addCheck(
    Check(CheckLevel.Error, "Checking data quality")
      .isComplete("OrderID") // should never be NULL
      .isUnique("OrderID") // should not contain duplicates
      .isComplete("CountryName") // should never be NULL
      .isNonNegative("SellAmount") // should not contain negative values
    )

println(s"Verification Result: ${createVerificationSuite(ordersWithCountryName).run().status.toString}")


val duplicateCountries = countries.union(countries)
val ordersWithCountryNameDuplicate = orders.join(duplicateCountries, Seq("CountryCode")).drop("CountryCode")
ordersWithCountryNameDuplicate.show(5)


println(s"Verification Result: ${createVerificationSuite(ordersWithCountryNameDuplicate).run().status.toString}")


/*
if (verificationResult.status == CheckStatus.Success) {
  println("The data passed the test, everything is fine!")
} else {
  println("We found errors in the data:\n")

  val resultsForAllConstraints = verificationResult.checkResults
    .flatMap { case (_, checkResult) => checkResult.constraintResults }

  resultsForAllConstraints
    .filter { _.status != ConstraintStatus.Success }
    .foreach { result => println(s"${result.constraint}: ${result.message.get}") }
}
/*
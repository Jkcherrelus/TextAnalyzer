package org.analyzer

import scala.math.random
import org.apache.spark._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.lang.invoke.LambdaForm
import scala.collection.mutable.ListBuffer

object TextAnalyzer {


  def main(args: Array[String]) {
    //setting hadoop directory
    System.setProperty("hadoop.home.dir","c:\\hadoop");
    
    //comment next two lines if you want to see all the logs
    Logger.getLogger("org").setLevel(Level.ERROR);
    Logger.getLogger("akka").setLevel(Level.ERROR);
    
    //connecting to spark driver
    val conf = new SparkConf().setAppName("TextAnalyzer").setMaster("local");
    val spark = new SparkContext(conf);
    
    //load the file
    val lines = spark.textFile("basketBall_words_only.txt");
    
    //split into individual words 
    val words = lines.flatMap { line => line.split(" ") }.cache()
    
    //use mapreduce to count words  
    val pairs = words.map(word=> (word,1))
    val counts = pairs.reduceByKey((x,y) => x+y).cache()
    //count the amount of words
    val wordCount = counts.count()
    //take the top 3% of words
    val topPer = wordCount.*(.03).round.toInt
    //sort results by integer
    val result = counts.sortBy(x => -x._2).collect().take(4)
    //sort the results by percentage 
    val resultByPer = counts.sortBy(x => -x._2).collect().take(topPer)
    
    
    //display results
    print("Words that account for at least 3% are ")
    for(i <- 0 to result.length-1){
      printf("\"%s\", ", result(i)._1);
    }
    System.out.println()
    System.out.println()
    for(i <- 0 to result.length-1)
    {
      printf("\n%s appears %d times",result(i)._1,result(i)._2)
    }
    System.out.println()
    System.out.println()
    
                
    
    val nextWord = counts.sortBy(x => -x._2).collect().take(wordCount.toInt)
    val wordsCollect = words.collect()
    
    for(i <- 0 to nextWord.length-1)
    {
      val lis = new ListBuffer[String]()
      for(j <- 0 to wordsCollect.length-2)
      {
        if(wordsCollect(j) == nextWord(i)._1)
        {
          lis += wordsCollect(j+1)
          //printf("The word is %s\n",lis)
        }
      }
      val total = lis.toList
      val tot = spark.parallelize(total).map { x => (x,1) }
      val totalWordMap = tot.sortBy(x => -x._2).reduceByKey((x,y) => x+y)
      val totalWords = totalWordMap.collect().take(1).sortBy(x => x._2)
      
      totalWordMap.collect().take(10).sortBy(word => -word._2).take(1)
      .foreach{case(k) => printf("The word [%s] is followed by \"%s\" %s times\n",nextWord(i)._1,k._1,k._2)}
      
      //printf("The word %s is followed by \"%s\" %s times\n",nextWord(i)._1,totalWords(0)._1,totalWords(0)._2)

    }
    val list = new ListBuffer[String]()
    list += "basketball"
    list += "the"
    list += "competitive"
    
    
    for(k <- 0 to nextWord.length-1)
    {
      
      words.foreach { case(i) => printf("%s is followed by %s %s times",list(k)) }
      //printf(" The word after %s is [%s]\n",wordsCollect(i),wordsCollect(i+1))
    }
//    for(i <- 0 to nextWord.length-1)
//    {
//      //printf("[%s,%d]\n",result(i)._1,result(i)._2)
//      
//      for(j <- 0 to wordss.length-1)
//      {
//       // if(wordss(j) == nextWord(i)._1)
//       // {
//        // printf("The word after %s is [%s] \n",wordss(j),wordss(j+1))
//        // lis += wordss(j+1)
//         
//       // }
//      }
//      
//    }
    
  }
  

  
}
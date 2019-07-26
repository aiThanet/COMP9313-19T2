import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import scala.xml.XML
import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Set
import scalaj.http._
import play.api.libs.json._

object CaseIndex {
    def main(args: Array[String]) {
        
        // create an index
        val index_response = Http("http://localhost:9200/legal_idx").method("PUT").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString
        // create a new mapping
        val mapping_response = Http("http://localhost:9200/legal_idx/cases/_mapping?pretty").postData("""{"cases":{"properties":{"id":{"type":"text"},"name":{"type":"text"},"url":{"type":"text"},"catchphrase":{"type":"text"},"sentence":{"type":"text"},"person":{"type":"text"},"location":{"type":"text"},"organization":{"type":"text"}}}}""").method("PUT").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString
    
        // get input path from argument
        val inputFile = args(0)
        // get list of case files
        val all_files = getListOfFiles(inputFile)
        // processing each file
        all_files.foreach(file=>{
            println(file)
            // load XML
            val xml = XML.loadFile(file)

            // get filename
            val filename = file.getName().split("\\.")(0)

            // get all information from XML
            val name = (xml \ "name").text.filter(_ >= ' ')
            val url =  (xml \ "AustLII").text.filter(_ >= ' ')
            val catchphrases = (xml \ "catchphrases" \ "catchphrase")
            val catchphrase = new StringBuilder(""); 
            catchphrases.foreach(_catchphrase=>{
                catchphrase ++=  _catchphrase.text + " "
            })
            val sentences = (xml \ "sentences" \ "sentence")
            val sentence_list = new ListBuffer[String]()
            sentences.foreach(sentence => {
                sentence_list += sentence.text.replace("\"","\\\"")

            })

            // Create set to store each name entities
            val locations : Set[String] = Set()
            val people : Set[String] = Set()
            val organizations : Set[String] = Set()
            val general_terms : Set[String] = Set()
        

            // pass all sentences to Named Entity Recognition API
            val NLP_result = Http("""http://localhost:9000/?properties=%7B'annotators':'ner','ner.applyFineGrained':'false','outputFormat':'json'%7D""").postData(sentence_list.mkString(" ")).method("POST").header("Content-Type", "application/json").option(HttpOptions.readTimeout(60000)).asString.body
            // parse reponse to JSON object
            val NLP_json: JsValue = Json.parse(NLP_result)
            
            // get all entities
            val entitymentions = NLP_json \\ "entitymentions"
            entitymentions.foreach(entitymention=>{
                val text = entitymention \\ "text"
                val ner = entitymention \\ "ner"
            
                var idx = 0
                for(idx <- 0 until text.length){
                    if(ner(idx).toString == "\"PERSON\""){
                        people += text(idx).toString
                    } else if(ner(idx).toString == "\"LOCATION\""){
                        locations += text(idx).toString
                    } else if(ner(idx).toString == "\"ORGANIZATION\""){
                        organizations += text(idx).toString
                    }
                }
                
            })
                
    
            
            // convert to list
            val people_list = "[" + people.toList.mkString(",")+"]"
            val locations_list = "[" + locations.toList.mkString(",")+"]"
            val organizations_list = "[" + organizations.toList.mkString(",")+"]"
            val new_sentence_list = "[" + sentence_list.map(x=>"\"" + x.filter(_ >= ' ') + "\"").mkString(",") + "]"
           
            // create a new document
            val post_Data = s"""{"id":"${filename}","name":"${name}","url":"${url}","catchphrase":"${catchphrase.toString.filter(_ >= ' ')}","sentence":${new_sentence_list},"person":${people_list},"location":${locations_list},"organization":${organizations_list}}"""
            val new_document_result = Http("http://localhost:9200/legal_idx/cases/"+filename+"?pretty").postData(post_Data).method("PUT").header("Content-Type", "application/json").option(HttpOptions.readTimeout(10000)).asString

        })
    }
    
    // get list of files in a directory path
    def getListOfFiles(dir: String):List[File] = {
        val d = new File(dir)
        if (d.exists && d.isDirectory) {
            d.listFiles.filter(_.isFile).toList
        } else {
            List[File]()
        }
    }
}

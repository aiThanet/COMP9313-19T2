## Start CoreNLP Server

`java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000`

For java 9/10/11

`java -mx4g --add-modules java.se.ee -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000`

## Send NLP Request

`wget --post-data '' 'localhost:9000/?properties={"annotators": "tokenize,ssplit,pos,ner", "outputFormat": "json"}' -O -`

```
wget --post-data 'Sydney is a city of Australia. I went to Apple store. Criminal and law are subject.' 'localhost:9000/?properties={"annotators":"tokenize,ssplit,pos,ner","ner.applyFineGrained":"false","outputFormat":"json"}' -O -
```

```
curl -X POST "http://localhost:9000/?properties={'annotators':'tokenize,ssplit,pos,ner','ner.applyFineGrained':'false','outputFormat':'json'}" -H "Content-type: application/json" -d "Sydney is a city of Australia. I went to Apple store. Criminal and law are subject."
```

```
spark-submit --class "CaseIndex" --master local[2] JAR_FILE FULL_PATH_OF_DIRECTORY_WITH_CASE_FILES
```

spark-submit --class "CaseIndex" --packages org.scalaj:scalaj-http_2.11:2.4.2,org.scalatestplus.play:scalatestplus-play_2.11:4.0.3 --master local[2] ~/COMP9313/assignment3/CaseIndex/target/scala-2.11/caseindex_2.11-1.0.jar ~/COMP9313/assignment3/cases_test

spark-submit --class "CaseIndex" --packages org.scalaj:scalaj-http_2.11:2.4.1,org.scalatestplus.play:scalatestplus-play_2.11:4.0.3 --master local[2] JAR_FILE FULL_PATH_OF_DIRECTORY_WITH_CASE_FILES

```
wget --post-data 'An interlocutory application on behalf of the plaintiff, Pendant Software Pty Limited (\"Pendant Software\") for an order that the second defendant, Mr Berend Hoff, be restrained from taking any further step directly or indirectly in a proceeding/application commenced by him in the Takeovers Panel pursuant to the application made by him dated 25 May 2006.' 'localhost:9000/?properties={"annotators":"tokenize","outputFormat":"json"}' -O -
```

```


```

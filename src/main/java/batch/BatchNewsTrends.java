package batch;

import com.johnsnowlabs.nlp.DocumentAssembler;
import com.johnsnowlabs.nlp.Finisher;
import com.johnsnowlabs.nlp.annotators.LemmatizerModel;
import com.johnsnowlabs.nlp.annotators.StopWordsCleaner;
import com.johnsnowlabs.nlp.annotators.Tokenizer;
import com.johnsnowlabs.nlp.annotators.ner.NerConverter;
import com.johnsnowlabs.nlp.annotators.ner.dl.NerDLModel;
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel;
import org.apache.spark.SparkConf;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.storage.StorageLevel;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class BatchNewsTrends {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "C:\\");
        System.setProperty("java.library.path", "");

        // Optimized Spark configuration
        SparkConf conf = new SparkConf()
                .setAppName("BatchNewsTrends")
                .setMaster("local[*]")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.execution.arrow.pyspark.enabled", "true")
                .set("spark.sql.adaptive.skewJoin.enabled", "true");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> data = spark.read()
                .text("hdfs://localhost:9000/user/hadoop/news_output")
                .withColumnRenamed("value", "text")
                .filter(functions.length(functions.col("text")).gt(10))
                .withColumn("text", functions.regexp_replace(functions.col("text"), "[,\\.!?]", ""))
                .cache();




        // Create optimized NLP pipeline - only essential components
        DocumentAssembler documentAssembler = new DocumentAssembler();
        documentAssembler.setInputCol("text");
        documentAssembler.setOutputCol("document");

        Tokenizer tokenizer = new Tokenizer();
        tokenizer.setInputCols(new String[]{"document"});
        tokenizer.setOutputCol("token");

        // Use lighter stopwords cleaner with common English stopwords
        StopWordsCleaner stopWordsCleaner = new StopWordsCleaner();
        stopWordsCleaner.setInputCols(new String[]{"token"});
        stopWordsCleaner.setOutputCol("cleanTokens");
        stopWordsCleaner.setCaseSensitive(false);

        // Lemmatization Stage
        LemmatizerModel lemmatizer = LemmatizerModel.pretrained();
        lemmatizer.setInputCols(new String[]{"cleanTokens"});
        lemmatizer.setOutputCol("lemma");

        Finisher finisher = new Finisher()
                .setInputCols(new String[]{"lemma"})
                .setOutputCols(new String[]{"finished_lemma"})
                .setOutputAsArray(true)
                .setCleanAnnotations(false);

        // Create basic pipeline first (without heavy NER)
        Pipeline basicPipeline = new Pipeline().setStages(new PipelineStage[]{
                documentAssembler,
                tokenizer,
                stopWordsCleaner,
                lemmatizer,
                finisher
        });

        // Process with basic pipeline
        PipelineModel basicModel = basicPipeline.fit(data);
        Dataset<Row> processedData = basicModel.transform(data)
                .persist(StorageLevel.MEMORY_AND_DISK_SER()); // Persist processed data

        // Extract and count word frequencies efficiently
        Dataset<Row> lemmas = processedData
                .selectExpr("explode(finished_lemma) as word")
                .filter(functions.length(functions.col("word")).gt(2)) // Filter short words
                .filter(functions.col("word").rlike("^[a-zA-Z]+$")); // Only alphabetic words

        Dataset<Row> wordCounts = lemmas
                .groupBy("word")
                .count()
                .orderBy(functions.desc("count"))
                .cache(); // Cache word counts for reuse

        // Get top words efficiently
        List<Row> topWords = wordCounts.limit(100).collectAsList();

        System.out.println("Top trending keywords from last week's news:");
        for (int i = 0; i < Math.min(100, topWords.size()); i++) {
            Row row = topWords.get(i);
            System.out.println(row.getString(0) + " -> " + row.getLong(1));
        }

        // Save word counts
        wordCounts.select("word", "count")
                .coalesce(1) // Reduce number of output files
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:9000/user/hadoop/keyword_counts");

        // Optimized keyword matching for trending news
        List<String> topKeywords = topWords.stream()
                .limit(10)
                .map(row -> row.getString(0).toLowerCase())
                .collect(Collectors.toList());

        final Broadcast<List<String>> broadcastKeywords = spark.sparkContext()
                .broadcast(topKeywords, scala.reflect.ClassTag$.MODULE$.apply(List.class));

        // Register optimized UDF
        spark.udf().register("countKeywordHits", (String text) -> {
            if (text == null || text.isEmpty()) return 0;

            String lowerText = text.toLowerCase();
            List<String> keywords = broadcastKeywords.value();
            int count = 0;

            for (String keyword : keywords) {
                if (lowerText.contains(keyword)) {
                    count++;
                }
            }
            return count;
        }, org.apache.spark.sql.types.DataTypes.IntegerType);

        // Score and filter news articles
        Dataset<Row> scoredNews = data
                .withColumn("keyword_hits", functions.callUDF("countKeywordHits", functions.col("text")))
                .filter(functions.col("keyword_hits").gt(0))
                .orderBy(functions.desc("keyword_hits"));

        System.out.println("\nTop trending news (keyword match):");
        scoredNews.select("text", "keyword_hits").show(5, false);

        // Save trending news
        scoredNews.select("text", "keyword_hits")
                .coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:9000/user/hadoop/trending_simple_news");

        // Optional: Run NER only on top trending articles (much faster)
        System.out.println("\nRunning NER on top trending articles only...");
        Dataset<Row> topTrendingForNER = scoredNews.limit(100); // Only top 100 articles

        // NER pipeline for selected articles
        WordEmbeddingsModel wordEmbeddings = WordEmbeddingsModel.pretrained("glove_100d");
        wordEmbeddings.setInputCols(new String[]{"document", "token"});
        wordEmbeddings.setOutputCol("word_embeddings");

        // NER Stage
        NerDLModel nerModel = NerDLModel.pretrained();
        nerModel.setInputCols(new String[]{"document", "token", "word_embeddings"});
        nerModel.setOutputCol("ner");

        // NER Converter Stage
        NerConverter nerConverter = new NerConverter();
        nerConverter.setInputCols(new String[]{"document", "token", "ner"});
        nerConverter.setOutputCol("entities");

        Pipeline nerPipeline = new Pipeline().setStages(new PipelineStage[]{
                documentAssembler,
                tokenizer,
                wordEmbeddings,
                nerModel,
                nerConverter
        });

        PipelineModel nerPipelineModel = nerPipeline.fit(topTrendingForNER);
        Dataset<Row> nerResults = nerPipelineModel.transform(topTrendingForNER);

        // Extract entities from NER results
        Dataset<Row> entities = nerResults
                .selectExpr("explode(entities.result) as entity")
                .filter(functions.length(functions.col("entity")).gt(1))
                .groupBy("entity")
                .count()
                .orderBy(functions.desc("count"));

        System.out.println("\nTop Named Entities (from trending articles):");
        entities.show(10, false);

        // Save entities
        entities.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://localhost:9000/user/hadoop/entity_counts");

        // Clean up
        broadcastKeywords.destroy();
        spark.close();
    }
}
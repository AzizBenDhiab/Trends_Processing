package streaming;

import com.johnsnowlabs.nlp.DocumentAssembler;
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
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;

import java.util.concurrent.TimeoutException;

import static org.apache.spark.sql.functions.*;

public class StreamingNewsNER {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/rss_analytics";
    private static final String DB_USER = "postgres";
    private static final String DB_PASSWORD = "fawzi1234";
    private static final String DB_DRIVER = "org.postgresql.Driver";

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkConf conf = new SparkConf()
                .setAppName("StreamingNERProcessor")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.adaptive.enabled", "false")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "false");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Test PostgreSQL connection
        try {
            System.out.println("🔌 Testing PostgreSQL connection...");
            Dataset<Row> testConnection = spark.read()
                    .format("jdbc")
                    .option("url", DB_URL)
                    .option("dbtable", "(SELECT 1 as test) as test_table")
                    .option("user", DB_USER)
                    .option("password", DB_PASSWORD)
                    .option("driver", DB_DRIVER)
                    .load();
            testConnection.show();
            System.out.println("✅ PostgreSQL connection successful!");
        } catch (Exception e) {
            System.err.println("❌ PostgreSQL connection failed: " + e.getMessage());
            System.exit(1);
        }

        // Initialize NER Pipeline
        System.out.println("🔧 Initializing NER Pipeline...");
        PipelineModel nerPipeline = createNERPipeline(spark);
        System.out.println("✅ NER Pipeline ready!");

        System.out.println("🚀 Starting streaming on port 9999...");
        System.out.println("📡 Send articles via CLI now!");

        // Read streaming data
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        // Process articles with basic metadata
        Dataset<Row> articles = lines
                .withColumn("title", col("value"))
                .withColumn("received_date", current_timestamp())
                .withColumn("word_count", size(split(col("value"), "\\s+")))
                .withColumn("language", lit("fr"))
                .withColumn("processing_status", lit("completed"))
                .withColumn("unique_url", concat(lit("socket://streaming/"),
                        date_format(current_timestamp(), "yyyyMMddHHmmssSSS")))
                .withColumn("source", lit("CLI_Stream"));

        // Combined query: Save articles AND process NER entities in same batch
        StreamingQuery combinedQuery = articles
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    long count = batchDF.count();
                    System.out.println("\n=== 📝 BATCH " + batchId + " - PROCESSING " + count + " ARTICLES ===");
                    if (count > 0) {
                        try {
                            // 1. Save articles first
                            System.out.println("📊 Articles to save:");
                            batchDF.select("title", "source", "word_count").show(false);

                            Dataset<Row> articlesToSave = batchDF.select(
                                    col("title"),
                                    lit("Contenu généré automatiquement").as("content"),
                                    col("source"),
                                    col("unique_url").as("url"),
                                    col("received_date").as("published_date"),
                                    col("received_date"),
                                    current_timestamp().as("processed_date"),
                                    col("word_count"),
                                    col("language"),
                                    lit(null).cast("decimal(3,2)").as("sentiment_score"),
                                    col("processing_status")
                            );

                            articlesToSave.write()
                                    .format("jdbc")
                                    .option("url", DB_URL)
                                    .option("dbtable", "streaming_articles")
                                    .option("user", DB_USER)
                                    .option("password", DB_PASSWORD)
                                    .option("driver", DB_DRIVER)
                                    .mode("append")
                                    .save();
                            System.out.println("✅ " + count + " articles saved to streaming_articles!");

                            // 2. Now process NER on the same batch
                            System.out.println("\n🎯 PROCESSING NER FOR THE SAME BATCH...");
                            System.out.println("📝 Input data for NER:");
                            batchDF.select("title").show(false);

                            // Apply NER pipeline to extract entities
                            Dataset<Row> nerResults = nerPipeline.transform(
                                    batchDF.select(col("title").alias("text"))
                            );

                            System.out.println("🔍 NER pipeline applied successfully");
                            System.out.println("📊 NER results schema:");
                            nerResults.printSchema();

                            System.out.println("🔍 Raw entities column:");
                            nerResults.select("entities").show(false);

                            // Extract entities with metadata
                            Dataset<Row> entitiesWithMetadata = nerResults
                                    .select(
                                            col("entities.result").alias("entities"),
                                            col("entities.metadata").alias("metadata")
                                    );

                            System.out.println("📊 Entities and metadata:");
                            entitiesWithMetadata.show(false);

                            // Combine entities with their types using arrays_zip
                            Dataset<Row> entityMetadataPairs = entitiesWithMetadata
                                    .withColumn("entity_metadata", arrays_zip(
                                            col("entities"),
                                            col("metadata")
                                    ))
                                    .select(explode(col("entity_metadata")).alias("pair"));

                            // Extract entity name and type
                            Dataset<Row> extractedEntities = entityMetadataPairs
                                    .select(
                                            col("pair.entities").alias("entity_name"),
                                            col("pair.metadata.entity").alias("entity_type")
                                    )
                                    .filter(col("entity_name").isNotNull())
                                    .filter(length(col("entity_name")).gt(1))
                                    .withColumn("detected_at", current_timestamp())
                                    .withColumn("source", lit("CLI_Stream"))
                                    .withColumn("confidence", lit(0.8))
                                    .withColumn("frequency", lit(1));

                            long entityCount = extractedEntities.count();
                            System.out.println("📊 Entity count: " + entityCount);

                            if (entityCount > 0) {
                                System.out.println("🎯 Extracted " + entityCount + " entities:");
                                extractedEntities.select("entity_name", "entity_type").show(false);

                                // Save entities to database
                                extractedEntities.write()
                                        .format("jdbc")
                                        .option("url", DB_URL)
                                        .option("dbtable", "extracted_entities")
                                        .option("user", DB_USER)
                                        .option("password", DB_PASSWORD)
                                        .option("driver", DB_DRIVER)
                                        .mode("append")
                                        .save();
                                System.out.println("✅ " + entityCount + " entities saved to extracted_entities!");
                            } else {
                                System.out.println("ℹ️ No entities found in this batch");
                            }

                        } catch (Exception e) {
                            System.err.println("❌ Error processing batch:");
                            System.err.println("Details: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("3 seconds"))
                .start();

        // FIXED: Entity frequency aggregation for trending entities
        StreamingQuery entityTrends = articles
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    if (batchDF.count() > 0) {
                        try {
                            // Read recent entities from database for trend calculation
                            Dataset<Row> recentEntities = spark.read()
                                    .format("jdbc")
                                    .option("url", DB_URL)
                                    .option("dbtable", "(SELECT entity_name, entity_type, COUNT(*) as count FROM extracted_entities WHERE detected_at >= NOW() - INTERVAL '10 minutes' GROUP BY entity_name, entity_type ORDER BY count DESC LIMIT 20) as recent_trends")
                                    .option("user", DB_USER)
                                    .option("password", DB_PASSWORD)
                                    .option("driver", DB_DRIVER)
                                    .load();

                            if (recentEntities.count() > 0) {
                                Dataset<Row> trendingEntities = recentEntities
                                        .withColumn("trend_score", col("count").multiply(lit(1.5)))
                                        .withColumn("time_window", current_timestamp())
                                        .withColumn("window_duration", lit("10 minutes"))
                                        .withColumn("source", lit("CLI_Stream"));

                                System.out.println("\n=== 📈 TRENDING ENTITIES ===");
                                trendingEntities.select("entity_name", "entity_type", "count", "trend_score").show(false);

                                // Save trending entities
                                trendingEntities.write()
                                        .format("jdbc")
                                        .option("url", DB_URL)
                                        .option("dbtable", "trending_entities")
                                        .option("user", DB_USER)
                                        .option("password", DB_PASSWORD)
                                        .option("driver", DB_DRIVER)
                                        .mode("overwrite") // Replace trending data
                                        .save();
                                System.out.println("✅ Trending entities updated!");
                            }
                        } catch (Exception e) {
                            System.err.println("❌ Error processing entity trends: " + e.getMessage());
                        }
                    }
                })
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("15 seconds"))
                .start();

        System.out.println("\n🎯 === NER STREAMING STARTED SUCCESSFULLY! ===");
        System.out.println("📡 Send data via PowerShell:");
        System.out.println("$client = New-Object System.Net.Sockets.TcpClient; $client.Connect('localhost', 9999); $stream = $client.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $writer.WriteLine('Emmanuel Macron annonce une nouvelle réforme à Paris'); $writer.Flush(); $client.Close()");
        System.out.println("🔍 Check Grafana dashboard for real-time entity visualization!");
        System.out.println("==========================================\n");

        // Wait for termination
        try {
            combinedQuery.awaitTermination();
            entityTrends.awaitTermination();
        } catch (Exception e) {
            System.err.println("❌ Streaming error: " + e.getMessage());
        } finally {
            try {
                spark.stop();
            } catch (Exception e) {
                System.err.println("Warning: Error stopping Spark context");
            }
        }
    }

    /**
     * Create NER Pipeline for entity extraction
     */
    private static PipelineModel createNERPipeline(SparkSession spark) {
        try {
            System.out.println("📦 Creating Document Assembler...");
            // Document Assembler
            DocumentAssembler documentAssembler = (DocumentAssembler) new DocumentAssembler()
                    .setInputCol("text")
                    .setOutputCol("document");

            System.out.println("🔤 Creating Tokenizer...");
            // Tokenizer
            Tokenizer tokenizer = new Tokenizer();
            tokenizer.setInputCols(new String[]{"document"});
            tokenizer.setOutputCol("token");

            System.out.println("📊 Loading Word Embeddings (this may take a while)...");
            // Word Embeddings
            WordEmbeddingsModel wordEmbeddings = WordEmbeddingsModel.pretrained("glove_100d");
            wordEmbeddings.setInputCols(new String[]{"document", "token"});
            wordEmbeddings.setOutputCol("word_embeddings");
            System.out.println("✅ Word Embeddings loaded!");

            System.out.println("🧠 Loading NER Model (this may take a while)...");
            // NER Model
            NerDLModel nerModel = NerDLModel.pretrained();
            nerModel.setInputCols(new String[]{"document", "token", "word_embeddings"});
            nerModel.setOutputCol("ner");
            System.out.println("✅ NER Model loaded!");

            System.out.println("🔄 Creating NER Converter...");
            // NER Converter
            NerConverter nerConverter = new NerConverter();
            nerConverter.setInputCols(new String[]{"document", "token", "ner"});
            nerConverter.setOutputCol("entities");

            System.out.println("⚙️ Building pipeline...");
            // Create pipeline
            Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                    documentAssembler,
                    tokenizer,
                    wordEmbeddings,
                    nerModel,
                    nerConverter
            });

            System.out.println("🧪 Creating test data for pipeline fitting...");
            // Create a dummy DataFrame to fit the pipeline
            Dataset<Row> dummyData = spark.createDataFrame(
                    java.util.Arrays.asList(
                            RowFactory.create("Emmanuel Macron visite Paris aujourd'hui")
                    ),
                    org.apache.spark.sql.types.DataTypes.createStructType(
                            new org.apache.spark.sql.types.StructField[]{
                                    org.apache.spark.sql.types.DataTypes.createStructField("text",
                                            org.apache.spark.sql.types.DataTypes.StringType, false)
                            }
                    )
            );

            System.out.println("🏗️ Fitting pipeline (this may take a while)...");
            PipelineModel pipelineModel = pipeline.fit(dummyData);

            System.out.println("🧪 Testing pipeline with dummy data...");
            Dataset<Row> testResult = pipelineModel.transform(dummyData);
            System.out.println("📊 Test result schema:");
            testResult.printSchema();

            System.out.println("🔍 Test entities found:");
            testResult.select("entities.result").show(false);

            System.out.println("✅ NER Pipeline created and tested successfully!");
            return pipelineModel;

        } catch (Exception e) {
            System.err.println("❌ Failed to create NER pipeline: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("NER Pipeline creation failed", e);
        }
    }
}
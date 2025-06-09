package streaming;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import java.util.concurrent.TimeoutException;
import static org.apache.spark.sql.functions.*;

public class StreamingNewsTrends {

    private static final String DB_URL = "jdbc:postgresql://localhost:5432/rss_analytics";
    private static final String DB_USER = "postgres"; // Changé pour simplifier
    private static final String DB_PASSWORD = "rabeb2002"; // CHANGEZ ICI votre mot de passe
    private static final String DB_DRIVER = "org.postgresql.Driver";

    public static void main(String[] args) throws StreamingQueryException, TimeoutException {
        System.setProperty("hadoop.home.dir", "C:\\hadoop");

        SparkConf conf = new SparkConf()
                .setAppName("StreamingRSSProcessor")
                .setMaster("local[*]");

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

        System.out.println("🚀 Starting streaming on port 9999...");
        System.out.println("📡 Send articles via CLI now!");

        // Read streaming data
        Dataset<Row> lines = spark.readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .option("includeTimestamp", true)
                .load();

        // Enhanced processing avec topic detection
        Dataset<Row> articles = lines
                .withColumn("title", col("value"))
                .withColumn("received_date", current_timestamp())
                .withColumn("word_count", size(split(col("value"), "\\s+")))
                .withColumn("language", lit("fr"))
                .withColumn("processing_status", lit("completed"))
                // CORRECTION 1: URL unique avec timestamp
                .withColumn("unique_url", concat(lit("socket://streaming/"),
                        date_format(current_timestamp(), "yyyyMMddHHmmssSSS")))
                // CORRECTION 2: Topic detection simple
                .withColumn("detected_topic",
                        when(lower(col("title")).contains("intelligence")
                                .or(lower(col("title")).contains("ia"))
                                .or(lower(col("title")).contains("artificielle")), "intelligence_artificielle")
                                .when(lower(col("title")).contains("climat")
                                        .or(lower(col("title")).contains("environnement"))
                                        .or(lower(col("title")).contains("écologie")), "climat_environnement")
                                .when(lower(col("title")).contains("économie")
                                        .or(lower(col("title")).contains("finance"))
                                        .or(lower(col("title")).contains("bourse")), "économie_finance")
                                .when(lower(col("title")).contains("sport")
                                        .or(lower(col("title")).contains("football"))
                                        .or(lower(col("title")).contains("tennis")), "sport")
                                .when(lower(col("title")).contains("santé")
                                        .or(lower(col("title")).contains("médecine"))
                                        .or(lower(col("title")).contains("covid")), "santé_médecine")
                                .when(lower(col("title")).contains("technologie")
                                        .or(lower(col("title")).contains("tech"))
                                        .or(lower(col("title")).contains("innovation")), "technologie")
                                .otherwise("général"));
        // CORRECTION 3: Save articles to PostgreSQL avec gestion d'erreurs
        StreamingQuery articlesQuery = articles
                .select(
                        col("title"),
                        lit("Contenu généré automatiquement").as("content"),
                        col("detected_topic").as("topic"),
                        lit("CLI_Stream").as("source"),
                        col("unique_url").as("url"), // URL unique maintenant
                        col("received_date").as("published_date"),
                        col("received_date"),
                        current_timestamp().as("processed_date"),
                        col("word_count"),
                        col("language"),
                        lit(null).cast("decimal(3,2)").as("sentiment_score"),
                        col("processing_status")
                )
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    long count = batchDF.count();
                    System.out.println("\n=== 📝 BATCH " + batchId + " - PROCESSING " + count + " ARTICLES ===");
                    if (count > 0) {
                        try {
                            System.out.println("📊 Articles to save:");
                            batchDF.select("title", "topic", "source", "word_count").show(false);

                            batchDF.write()
                                    .format("jdbc")
                                    .option("url", DB_URL)
                                    .option("dbtable", "streaming_articles")
                                    .option("user", DB_USER)
                                    .option("password", DB_PASSWORD)
                                    .option("driver", DB_DRIVER)
                                    .mode("append")
                                    .save();
                            System.out.println("✅ " + count + " articles saved to streaming_articles!");
                        } catch (Exception e) {
                            System.err.println("❌ ERROR saving articles:");
                            System.err.println("Details: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .outputMode("append")
                .trigger(Trigger.ProcessingTime("3 seconds"))
                .start();

        // CORRECTION 4: Trending topics avec agrégation par fenêtre
        Dataset<Row> trendingTopics = articles
                .groupBy(
                        col("detected_topic"),
                        window(col("received_date"), "1 minutes") // Fenêtre d'1 minute
                )
                .agg(
                        count("*").as("count"),
                        max("received_date").as("last_seen")
                )
                .select(
                        col("detected_topic").as("topic"),
                        col("count"),
                        col("count").cast("double").multiply(lit(2.0)).as("trend_score"), // Score basé sur count
                        col("window.start").as("time_window"),
                        lit("1 minutes").as("window_duration"),
                        col("window.start").as("first_seen"),
                        col("last_seen"),
                        col("count").as("peak_count"),
                        array(lit("CLI_Stream")).as("sources")
                );

        StreamingQuery topicsQuery = trendingTopics
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    long count = batchDF.count();
                    if (count > 0) {
                        System.out.println("\n=== 📈 SAVING " + count + " TRENDING TOPICS ===");
                        batchDF.select("topic", "count", "trend_score").show(false);
                        try {
                            batchDF.write()
                                    .format("jdbc")
                                    .option("url", DB_URL)
                                    .option("dbtable", "trending_topics")
                                    .option("user", DB_USER)
                                    .option("password", DB_PASSWORD)
                                    .option("driver", DB_DRIVER)
                                    .mode("append")
                                    .save();
                            System.out.println("✅ " + count + " trending topics saved!");
                        } catch (Exception e) {
                            System.err.println("❌ Error saving trending topics: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .outputMode("complete")
                .trigger(Trigger.ProcessingTime("10 seconds"))
                .start();

        // CORRECTION 5: Word cloud data
        Dataset<Row> wordCloudData = articles
                .withColumn("clean_title",
                        regexp_replace(lower(col("title")), "[^a-zA-ZàáâäèéêëìíîïòóôöùúûüÿñçÀÁÂÄÈÉÊËÌÍÎÏÒÓÔÖÙÚÛÜŸÑÇ\\s]", ""))
                .withColumn("words", split(col("clean_title"), "\\s+"))
                .select(
                        explode(col("words")).as("word"),
                        col("detected_topic").as("topic"),
                        current_timestamp().as("created_at")
                )
                .filter(length(col("word")).gt(3)) // Mots de plus de 3 caractères
                .filter(not(col("word").rlike("^(le|la|les|un|une|des|de|du|et|est|sont|dans|pour|avec|sur|par|mais|ou|car|donc|puis|ainsi|mais|très|plus|bien|tout|tous|cette|cette|ces|ses|mes|nos|vos|leurs)$")))
                .groupBy("word", "topic")
                .agg(count("*").as("frequency"))
                .filter(col("frequency").gt(0))
                .withColumn("weight", col("frequency").cast("double"))
                .withColumn("font_size",
                        when(col("frequency").gt(10), 36)
                                .when(col("frequency").gt(5), 28)
                                .when(col("frequency").gt(2), 20)
                                .otherwise(16))
                .withColumn("color",
                        when(col("frequency").gt(10), "#FF6B6B")
                                .when(col("frequency").gt(5), "#4ECDC4")
                                .when(col("frequency").gt(2), "#45B7D1")
                                .otherwise("#95E1D3"))
                .withColumn("category", lit("trending"))
                .withColumn("expires_at",
                        expr("current_timestamp() + interval '2 hours'"));

        StreamingQuery wordCloudQuery = wordCloudData
                .writeStream()
                .foreachBatch((batchDF, batchId) -> {
                    long count = batchDF.count();
                    if (count > 0) {
                        System.out.println("\n=== 🎨 SAVING " + count + " WORD CLOUD ENTRIES ===");
                        batchDF.select("word", "frequency", "topic", "font_size").show(false);
                        try {
                            batchDF.write()
                                    .format("jdbc")
                                    .option("url", DB_URL)
                                    .option("dbtable", "wordcloud_realtime")
                                    .option("user", DB_USER)
                                    .option("password", DB_PASSWORD)
                                    .option("driver", DB_DRIVER)
                                    .mode("append")
                                    .save();
                            System.out.println("✅ " + count + " word cloud entries saved!");
                        } catch (Exception e) {
                            System.err.println("❌ Error saving word cloud: " + e.getMessage());
                            e.printStackTrace();
                        }
                    }
                })
                .outputMode("complete")
                .trigger(Trigger.ProcessingTime("5 seconds"))
                .start();

        System.out.println("\n🎯 === STREAMING STARTED SUCCESSFULLY! ===");
        System.out.println("📡 Send data via PowerShell:");
        System.out.println("$client = New-Object System.Net.Sockets.TcpClient; $client.Connect('localhost', 9999); $stream = $client.GetStream(); $writer = New-Object System.IO.StreamWriter($stream); $writer.WriteLine('Intelligence artificielle révolutionne la médecine'); $writer.Flush(); $client.Close()");
        System.out.println("🔍 Check Grafana dashboard for real-time updates!");
        System.out.println("==========================================\n");

        // Attendre que toutes les requêtes se terminent
        try {
            articlesQuery.awaitTermination();
        } finally {
            spark.stop();
        }
    }
}
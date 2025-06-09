package batch;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import java.util.Properties;

public class HDFSToGrafanaProcessor {

    public static void main(String[] args) {
        // Fix Hadoop native library issue for Windows
        System.setProperty("hadoop.home.dir", "C:\\");
        System.setProperty("java.library.path", "");

        // Initialize Spark session
        SparkConf conf = new SparkConf()
                .setAppName("HDFS-to-Grafana-Processor")
                .setMaster("local[*]")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.adaptive.enabled", "false");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                .getOrCreate();

        try {
            processNewsSimpleData(spark);
            processKeywordData(spark);
            processEntityData(spark);
            createGrafanaViews(spark);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            spark.close();
        }
    }

    private static void processNewsSimpleData(SparkSession spark) {
        Dataset<Row> topSimpleNews = spark.read()
                .parquet("hdfs://localhost:9000/user/hadoop/trending_simple_news")
                .withColumn("timestamp", functions.unix_timestamp().cast(DataTypes.LongType).multiply(1000))
                .withColumn("date", functions.current_date())
                .withColumn("hour", functions.hour(functions.current_timestamp()));

        Dataset<Row> trendingNewsWithSummary = topSimpleNews
                .withColumn("news_title", functions.substring(functions.col("text"), 1, 50))
                .select("news_title", "keyword_hits", "timestamp", "date", "hour")
                .orderBy(functions.desc("keyword_hits"))
                .limit(10);;

        trendingNewsWithSummary.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/trending_simple_news");
    }

    private static void processKeywordData(SparkSession spark) {
        System.out.println("Processing keyword data from HDFS...");

        Dataset<Row> keywordData = spark.read()
                .parquet("hdfs://localhost:9000/user/hadoop/keyword_counts");

        Dataset<Row> keywordTrends = keywordData
                .withColumn("count", new Column("count").cast(DataTypes.LongType))
                .withColumn("timestamp", functions.unix_timestamp(functions.current_timestamp()).multiply(1000))
                .withColumn("date", functions.current_date())
                .withColumn("hour", functions.hour(functions.current_timestamp()))
                .filter(new Column("count").gt(1L))
                .orderBy(functions.desc("count"))
                .limit(20);;

        keywordTrends.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/keyword_trends");

        Dataset<Row> hourlyKeywords = keywordTrends
                .groupBy("word", "date", "hour")
                .agg(functions.sum("count").alias("hourly_count"))
                .withColumn("datetime", functions.concat(
                        functions.col("date"),
                        functions.lit(" "),
                        functions.col("hour"),
                        functions.lit(":00:00")
                ))
                .select("word", "hourly_count", "datetime");

        hourlyKeywords.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/hourly_keyword_trends");

        Dataset<Row> topKeywords = keywordTrends
                .select("word", "count", "timestamp")
                .limit(50);

        topKeywords.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/top_keywords");

        System.out.println("Keyword data processing completed!");
        keywordTrends.show(10, false);
    }

    private static void processEntityData(SparkSession spark) {
        System.out.println("Processing entity data from HDFS...");

        Dataset<Row> entityData = spark.read()
                .parquet("hdfs://localhost:9000/user/hadoop/entity_counts");

        Dataset<Row> entityTrends = entityData
                .groupBy("entity", "entity_type") // Group to remove duplicates
                .agg(functions.sum("count").alias("count")) // Sum count across days
                .withColumn("timestamp", functions.unix_timestamp().cast(DataTypes.LongType).multiply(1000))
                .withColumn("date", functions.current_date())
                .withColumn("hour", functions.hour(functions.current_timestamp()))
                .orderBy(functions.desc("count"));

        entityTrends.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_trends");

        Dataset<Row> entityTypeDistribution = entityTrends
                .groupBy("entity_type", "date")
                .agg(
                        functions.count("entity").alias("unique_entities"),
                        functions.sum("count").alias("total_mentions"),
                        functions.avg("count").alias("avg_mentions_per_entity")
                )
                .withColumn("timestamp", functions.current_timestamp());

        entityTypeDistribution.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_type_distribution");

        WindowSpec windowSpec = Window
                .partitionBy("entity_type")
                .orderBy(functions.desc("count"));

        Dataset<Row> topEntitiesByType = entityTrends
                .withColumn("rank", functions.row_number().over(windowSpec))
                .filter(functions.col("rank").leq(10))
                .select("entity", "entity_type", "count", "timestamp", "rank");

        topEntitiesByType.coalesce(1)
                .write()
                .mode(SaveMode.Overwrite)
                .json("hdfs://localhost:9000/user/hadoop/grafana_data/top_entities_by_type");

        System.out.println("Entity data processing completed!");
        entityTrends.show(10, false);
    }



    private static void createGrafanaViews(SparkSession spark) {
        System.out.println("Creating aggregated views for Grafana...");

        try {
            // ✅ Read data
            Dataset<Row> keywords = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/keyword_trends");
            Dataset<Row> entities = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_trends");
            Dataset<Row> trendingNews = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/trending_news"); // ✅ Ajout des trending news

            // ✅ Register temp views
            keywords.createOrReplaceTempView("keywords_temp");
            entities.createOrReplaceTempView("entities_temp");
            trendingNews.createOrReplaceTempView("trending_news_temp"); // ✅ Nouvelle vue temporaire

            // ✅ Enhanced summary stats query avec trending news
            String summaryStatsQuery =
                    "SELECT current_timestamp() as timestamp, " +
                            "current_date() as date, " +
                            "(SELECT COUNT(DISTINCT word) FROM keywords_temp) as unique_keywords, " +
                            "(SELECT COUNT(DISTINCT entity) FROM entities_temp) as unique_entities, " +
                            "(SELECT COUNT(*) FROM trending_news_temp) as trending_news_count, " +
                            "(SELECT SUM(count) FROM keywords_temp) as total_keyword_mentions, " +
                            "(SELECT SUM(count) FROM entities_temp) as total_entity_mentions, " +
                            "(SELECT AVG(similarity) FROM trending_news_temp) as avg_news_similarity, " +
                            "(SELECT MAX(count) FROM keywords_temp) as max_keyword_frequency, " +
                            "(SELECT MAX(count) FROM entities_temp) as max_entity_frequency, " +
                            "(SELECT MAX(similarity) FROM trending_news_temp) as max_news_similarity";

            Dataset<Row> summaryStats = spark.sql(summaryStatsQuery);

            // ✅ Save enhanced summary stats
            summaryStats.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/summary_stats");

            // ✅ Enhanced dashboard summary avec trending news
            Dataset<Row> dashboardSummary = keywords
                    .agg(
                            functions.countDistinct("word").alias("unique_keywords"),
                            functions.sum("count").alias("total_keyword_mentions"),
                            functions.max("count").alias("max_keyword_frequency"),
                            functions.avg("count").alias("avg_keyword_frequency")
                    )
                    .crossJoin(
                            trendingNews.agg(
                                    functions.count("text").alias("trending_news_count"),
                                    functions.avg("similarity").alias("avg_news_similarity"),
                                    functions.max("similarity").alias("max_news_similarity")
                            )
                    )
                    .withColumn("timestamp", functions.current_timestamp())
                    .withColumn("date", functions.current_date());

            dashboardSummary.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/dashboard_summary");

            // ✅ Enhanced trend comparison avec trending news
            Dataset<Row> trendComparison = keywords
                    .select("word", "count")
                    .limit(15)
                    .withColumn("type", functions.lit("keyword"))
                    .withColumn("score", functions.col("count").cast("double"))
                    .withColumnRenamed("word", "name")
                    .select("name", "score", "type")
                    .union(
                            entities.select("entity", "count")
                                    .limit(15)
                                    .withColumn("type", functions.lit("entity"))
                                    .withColumn("score", functions.col("count").cast("double"))
                                    .withColumnRenamed("entity", "name")
                                    .select("name", "score", "type")
                    )
                    .union(
                            trendingNews
                                    .withColumn("news_title",
                                            functions.substring(functions.col("text"), 1, 50)
                                    )
                                    .select("news_title", "similarity")
                                    .limit(10)
                                    .withColumn("type", functions.lit("trending_news"))
                                    .withColumn("score", functions.col("similarity").multiply(100)) // Normaliser le score
                                    .withColumnRenamed("news_title", "name")
                                    .select("name", "score", "type")
                    )
                    .withColumn("timestamp", functions.current_timestamp());

            trendComparison.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/trend_comparison");

            // ✅ Nouvelle vue combinée pour dashboard principal
            Dataset<Row> mainDashboardData = spark.sql(
                    "SELECT " +
                            "current_timestamp() as timestamp, " +
                            "current_date() as date, " +
                            "'summary' as metric_type, " +
                            "(SELECT COUNT(DISTINCT word) FROM keywords_temp) as keywords_count, " +
                            "(SELECT COUNT(DISTINCT entity) FROM entities_temp) as entities_count, " +
                            "(SELECT COUNT(*) FROM trending_news_temp) as news_count, " +
                            "(SELECT ROUND(AVG(similarity), 4) FROM trending_news_temp) as avg_relevance_score"
            );

            mainDashboardData.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/main_dashboard");

            System.out.println("Enhanced Grafana views with trending news created successfully!");

        } catch (Exception e) {
            System.err.println("Error creating Grafana views: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
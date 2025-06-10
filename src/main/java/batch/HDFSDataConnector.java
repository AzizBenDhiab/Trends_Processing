package batch;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.apache.spark.sql.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.time.LocalDateTime;

@SpringBootApplication
@EnableScheduling
@RestController
@CrossOrigin(origins = "*")
public class HDFSDataConnector {

    private SparkSession spark;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) {
        SpringApplication.run(HDFSDataConnector.class, args);
    }

    @PostConstruct
    public void initSpark() {
        System.setProperty("hadoop.home.dir", "C:\\");
        System.setProperty("java.library.path", "");
        this.spark = SparkSession.builder()
                .appName("HDFS-Grafana-Connector")
                .master("local[*]")
                .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                .getOrCreate();
    }

    @GetMapping("/")
    public Map<String, String> root() {
        Map<String, String> map = new HashMap<>();
        map.put("status", "OK");
        map.put("message", "HDFS Data Connector for Grafana");
        return map;
    }

    @GetMapping("/search")
    public List<String> search() {
        return Arrays.asList(
                "keyword_trends",
                "entity_trends",
                "entity_type_distribution",
                "top_keywords",
                "top_entities",
                "dashboard_summary",
                "trend_comparison"
        );
    }

    @GetMapping("/query")
    public List<Map<String, Object>> query(@RequestParam List<String> targets) {
        List<Map<String, Object>> response = new ArrayList<>();
        for (String targetName : targets) {
            switch (targetName) {
                case "keyword_trends": response.add(getKeywordTrends()); break;
                case "entity_trends": response.add(getEntityTrends()); break;
                case "entity_type_distribution": response.add(getEntityTypeDistribution()); break;
                case "top_keywords": response.add(getTopKeywords()); break;
                case "top_entities": response.add(getTopEntities()); break;
                case "dashboard_summary": response.add(getDashboardSummary()); break;
                case "trend_comparison": response.add(getTrendComparison()); break;
                case "trending_simple_news": response.add(gettrending_news()); break;
                case "entites_count": response.add(getentity_count()); break;
                case "country_counts": response.add(getCountryCounts()); break;
            }
        }
        return response;
    }

    @GetMapping("/annotations")
    public List<Map<String, Object>> annotations() {
        return new ArrayList<>();
    }

    private Map<String, Object> getKeywordTrends() {
        return getKeyWordsTrend();
    }

    private Map<String, Object> getEntityTrends() {
        return queryTopEntityTrends(20);
    }private Map<String, Object> getentity_count() {
        return queryEntityCount(10);
    }

    private Map<String, Object> getTopKeywords() {
        return queryData("top_keywords", "count");
    }

    private Map<String, Object> getTopEntities() {
        return queryData("entity_trends", "count");
    }

    private Map<String, Object> getDashboardSummary() {
        return queryData("dashboard_summary", "unique_keywords");
    }

    private Map<String, Object> getTrendComparison() {
        return queryData("trend_comparison", "count");
    }
    private Map<String, Object> gettrending_news() {
        return queryTrendingSimpleNews();
    }
    private Map<String, Object> getCountryCounts() {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "country_counts");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/country_trends")
                    .limit(50);
            data.show();
            List<Map<String, Object>> datapoints = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            int interval = 1000;
            int index = 0;

            for (Row row : data.collectAsList()) {
                String country = row.getAs("country");
                Long count = row.getAs("count");

                if (country != null && count != null) {
                    Map<String, Object> point = new HashMap<>();
                    point.put("label", country);
                    point.put("value", count);
                    point.put("timestamp", baseTime + (index++ * interval));
                    datapoints.add(point);
                }
            }

            result.put("datapoints", datapoints);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("datapoints", new ArrayList<>());
        }

        return result;
    }


    private Map<String, Object> getEntityTypeDistribution() {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "entity_type_distribution");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_type_distribution");

            List<Map<String, Object>> series = new ArrayList<>();
            for (Row row : data.collectAsList()) {
                String target = row.getAs("entity_type");
                Object total = row.getAs("total_mentions");
                if (total instanceof Number) {
                    Map<String, Object> entry = new HashMap<>();
                    entry.put("target", target);
                    List<List<Object>> datapoints = new ArrayList<>();
                    datapoints.add(Arrays.asList(((Number) total).doubleValue(), System.currentTimeMillis()));
                    entry.put("datapoints", datapoints);
                    series.add(entry);
                }
            }
            return series.isEmpty() ? result : series.get(0);
        } catch (Exception e) {
            result.put("datapoints", new ArrayList<>());
            return result;
        }
    }

    private Map<String, Object> queryTrendingSimpleNews() {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "trending_simple_news");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/trending_simple_news")
                    .limit(10);

            List<Row> rows = data.collectAsList();
            long totalHits = rows.stream()
                    .mapToLong(row -> {
                        Long hits = row.getAs("keyword_hits");
                        return hits != null ? hits : 0;
                    })
                    .sum();

            List<Map<String, Object>> datapoints = new ArrayList<>();

            for (Row row : rows) {
                String title = row.getAs("news_title");
                Long hits = row.getAs("keyword_hits");

                if (title != null && hits != null && hits > 0 && totalHits > 0) {
                    double percentage = (double) hits / totalHits * 100;

                    Map<String, Object> point = new HashMap<>();
                    point.put("text", title);
                    point.put("value", Math.round(percentage * 100.0) / 100.0); // Round to 2 decimals

                    datapoints.add(point);
                }
            }

            result.put("datapoints", datapoints);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("datapoints", new ArrayList<Object>());
        }

        return result;
    }

    private Map<String, Object> queryTopEntityTrends(int limit) {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "entity_trends");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_trends")
                    .limit(limit);
            data.show();

            List<Map<String, Object>> datapoints = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            int interval = 1000;
            int index = 0;

            for (Row row : data.collectAsList()) {
                long value = row.getAs("count");
                String  label = row.getAs("entity");


                if ( label != null) {
                    Map<String, Object> point = new HashMap<>();
                    point.put("value", value);
                    point.put("label", label);

                    datapoints.add(point);

                }
            }

            result.put("datapoints", datapoints);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("datapoints", new ArrayList<Object>());
        }

        return result;
    }
    private Map<String, Object> queryEntityCount(int limit) {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "entity_trends");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/entity_trends")
                    .limit(limit);
            data.show();

            List<Map<String, Object>> datapoints = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            int interval = 1000;
            int index = 0;

            for (Row row : data.collectAsList()) {
                long value = row.getAs("count");
                String  label = row.getAs("entity");


                if ( label != null) {
                    Map<String, Object> point = new HashMap<>();
                    point.put("value", value);
                    point.put("label", label);

                    datapoints.add(point);

                }
            }

            result.put("datapoints", datapoints);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("datapoints", new ArrayList<Object>());
        }

        return result;
    }

    private Map<String, Object> getKeyWordsTrend() {
        Map<String, Object> result = new HashMap<>();
        result.put("target", "keyword_trends");

        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/keyword_trends")
                    .limit(50);

            List<Map<String, Object>> datapoints = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            int interval = 1000;
            int index = 0;

            for (Row row : data.collectAsList()) {
                String word = row.getAs("word");
                Long  count = row.getAs("count");
                long ts = row.getAs("timestamp");

                if (word != null ) {
                    Map<String, Object> point = new HashMap<>();
                    point.put("word", word);
                    point.put("value", count);
                    point.put("timestamp", ts != 0 ? ts : (baseTime + index * interval));
                    datapoints.add(point);
                    index++;
                }
            }

            result.put("datapoints", datapoints);
        } catch (Exception e) {
            e.printStackTrace();
            result.put("datapoints", new ArrayList<Object>());
        }

        return result;
    }



    private Map<String, Object> queryData(String pathKey, String valueField) {
        Map<String, Object> result = new HashMap<>();
        result.put("target", pathKey);


        try {
            Dataset<Row> data = spark.read()
                    .json("hdfs://localhost:9000/user/hadoop/grafana_data/" + pathKey)
                    .limit(100);
            data.show();

            List<Map<String, Object>> datapoints = new ArrayList<>();
            long baseTime = System.currentTimeMillis();
            int intervalMillis = 1000;
            int index = 0;

            for (Row row : data.collectAsList()) {
                Object value = row.getAs(valueField);
                Object label = row.schema().fieldNames().length > 3 ? row.get(2) : null; // assume first column is label if available
                if (value instanceof Number) {
                    double number = ((Number) value).doubleValue();
                    if (!Double.isNaN(number) && !Double.isInfinite(number)) {
                        long timestamp = baseTime + (index * intervalMillis);
                        Map<String, Object> point = new HashMap<>();
                        point.put("value", number);
                        point.put("timestamp", timestamp);
                        if (label != null) {
                            point.put("label", label.toString());
                        }
                        datapoints.add(point);
                        index++;
                    }
                }
            }
            result.put("datapoints", datapoints);
        } catch (Exception e) {
            result.put("datapoints", new ArrayList<>());
        }

        return result;
    }



    @Scheduled(fixedRate = 300_000)
    public void refreshData() {
        System.out.println("Refreshing data cache at: " + LocalDateTime.now());
    }

    @PreDestroy
    public void cleanup() {
        if (spark != null) spark.close();
    }
}
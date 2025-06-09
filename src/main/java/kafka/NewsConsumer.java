package kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class NewsConsumer {

    private static final String TOPIC_NAME = "news";
    private static final String BOOTSTRAP_SERVERS = "192.168.100.12:9092";
    private static final String GROUP_ID = "news-consumer-group";
    // WebHDFS settings
    private static final String WEBHDFS_BASE = "http://localhost:9870/webhdfs/v1";
    private static final String OUTPUT_PATH = "/user/news_output.txt";

    public static void main(String[] args) {
        // Configure Kafka properties
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = null;

        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(TOPIC_NAME));

            ObjectMapper mapper = new ObjectMapper();

            System.out.println("Waiting for news messages...");
            System.out.println("WebHDFS endpoint: " + WEBHDFS_BASE);
            System.out.println("Output path: " + OUTPUT_PATH);

            // Test WebHDFS connection
            testWebHDFSConnection();

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                if (!records.isEmpty()) {
                    System.out.println("\n=== Processing " + records.count() + " records ===");

                    // Process and display messages
                    for (ConsumerRecord<String, String> record : records) {
                        System.out.println("\n--- New Message Received ---");
                        processAndDisplayMessage(record.value(), mapper);
                    }

                    // Write to HDFS using direct WebHDFS API
                    writeToWebHDFS(records, mapper);
                }
            }
        } catch (Exception e) {
            System.err.println("Error in main processing: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Clean up resources
            if (consumer != null) {
                try {
                    consumer.close();
                } catch (Exception e) {
                    System.err.println("Error closing consumer: " + e.getMessage());
                }
            }
        }
    }

    private static void testWebHDFSConnection() {
        try {
            String testUrl = WEBHDFS_BASE + "/?op=LISTSTATUS";
            URL url = new URL(testUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("✓ WebHDFS connection successful");
            } else {
                System.err.println("✗ WebHDFS connection failed: " + responseCode);
            }
            conn.disconnect();
        } catch (Exception e) {
            System.err.println("✗ WebHDFS connection test failed: " + e.getMessage());
        }
    }

    private static void processAndDisplayMessage(String jsonValue, ObjectMapper mapper) {
        try {
            JsonNode root = mapper.readTree(jsonValue);
            JsonNode articles = root.get("articles");

            if (articles != null && articles.isArray()) {
                for (JsonNode article : articles) {
                    String title = article.path("title").asText("No title");
                    String author = article.path("author").asText("Unknown");
                    String description = article.path("description").asText("No description");
                    String url = article.path("url").asText("No URL");
                    String publishedAt = article.path("publishedAt").asText("Unknown date");

                    System.out.println("Title: " + title);
                    System.out.println("Author: " + author);
                    System.out.println("Description: " + description);
                    System.out.println("URL: " + url);
                    System.out.println("Published At: " + publishedAt);
                    System.out.println("------------------------------------------");
                }
            } else {
                System.out.println("No articles found in the message.");
            }
        } catch (Exception e) {
            System.err.println("Error parsing JSON: " + e.getMessage());
        }
    }

    private static void writeToWebHDFS(ConsumerRecords<String, String> records, ObjectMapper mapper) {
        StringBuilder content = new StringBuilder();
        int articleCount = 0;

        // Collect all content to write
        for (ConsumerRecord<String, String> record : records) {
            String jsonValue = record.value();

            try {
                JsonNode root = mapper.readTree(jsonValue);
                JsonNode articles = root.get("articles");

                if (articles != null && articles.isArray()) {
                    for (JsonNode article : articles) {
                        String title = article.path("title").asText("No title");
                        String publishedAt = article.path("publishedAt").asText("Unknown date");

                        String line = String.format("Title: %s | Published At: %s%n", title, publishedAt);
                        content.append(line);
                        articleCount++;
                    }
                }
            } catch (Exception e) {
                System.err.println("Error parsing JSON for HDFS write: " + e.getMessage());
            }
        }

        if (content.length() > 0) {
            // Check if file exists and append or create
            if (fileExists(OUTPUT_PATH)) {
                appendToWebHDFS(OUTPUT_PATH, content.toString());
            } else {
                createFileInWebHDFS(OUTPUT_PATH, content.toString());
            }
            System.out.println("✓ Successfully wrote " + articleCount + " articles to HDFS");
        }
    }

    private static boolean fileExists(String filePath) {
        try {
            String statusUrl = WEBHDFS_BASE + filePath + "?op=GETFILESTATUS";
            URL url = new URL(statusUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");

            int responseCode = conn.getResponseCode();
            conn.disconnect();

            return responseCode == 200;
        } catch (Exception e) {
            return false;
        }
    }

    private static void createFileInWebHDFS(String filePath, String content) {
        try {
            // Step 1: Create file (get redirect URL)
            String createUrl = WEBHDFS_BASE + filePath + "?op=CREATE&overwrite=false";
            URL url = new URL(createUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setInstanceFollowRedirects(false);

            int responseCode = conn.getResponseCode();

            if (responseCode == 307) {
                String location = conn.getHeaderField("Location");
                // Replace internal hostname with localhost
                String modifiedLocation = location.replaceAll("hadoop-worker1:9864", "localhost:9864")
                        .replaceAll("hadoop-worker2:9864", "localhost:9864")
                        .replaceAll("hadoop-datanode:9864", "localhost:9864")
                        .replaceAll("datanode:9864", "localhost:9864");

                conn.disconnect();

                // Step 2: Write data to redirected URL
                writeDataToURL(modifiedLocation, content, "PUT");

            } else {
                System.err.println("Failed to create file, response code: " + responseCode);
                conn.disconnect();
            }

        } catch (Exception e) {
            System.err.println("Error creating file in WebHDFS: " + e.getMessage());
        }
    }

    private static void appendToWebHDFS(String filePath, String content) {
        try {
            // Step 1: Open file for append (get redirect URL)
            String appendUrl = WEBHDFS_BASE + filePath + "?op=APPEND";
            URL url = new URL(appendUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setInstanceFollowRedirects(false);

            int responseCode = conn.getResponseCode();

            if (responseCode == 307) {
                String location = conn.getHeaderField("Location");
                // Replace internal hostname with localhost
                String modifiedLocation = location.replaceAll("hadoop-worker1:9864", "localhost:9864")
                        .replaceAll("hadoop-datanode:9864", "localhost:9864")
                        .replaceAll("datanode:9864", "localhost:9864");

                conn.disconnect();

                // Step 2: Write data to redirected URL
                writeDataToURL(modifiedLocation, content, "POST");

            } else {
                System.err.println("Failed to append to file, response code: " + responseCode);
                conn.disconnect();
            }

        } catch (Exception e) {
            System.err.println("Error appending to WebHDFS: " + e.getMessage());
        }
    }

    private static void writeDataToURL(String dataUrl, String content, String method) {
        try {
            URL url = new URL(dataUrl);
            HttpURLConnection dataConn = (HttpURLConnection) url.openConnection();
            dataConn.setRequestMethod(method);
            dataConn.setDoOutput(true);
            dataConn.setRequestProperty("Content-Type", "application/octet-stream");

            // Write content
            try (OutputStream os = dataConn.getOutputStream()) {
                os.write(content.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }

            int dataResponseCode = dataConn.getResponseCode();

            if ((method.equals("PUT") && dataResponseCode == 201) ||
                    (method.equals("POST") && dataResponseCode == 200)) {
                System.out.println("✓ Successfully wrote data to HDFS via WebHDFS");
            } else {
                System.err.println("✗ Failed to write data to HDFS: " + dataResponseCode);
                // Read error response
                try (BufferedReader br = new BufferedReader(new InputStreamReader(dataConn.getErrorStream()))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        System.err.println("Error: " + line);
                    }
                }
            }

            dataConn.disconnect();

        } catch (Exception e) {
            System.err.println("Error writing data to URL: " + e.getMessage());
        }
    }
}

package kafka;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class NewsApiFetcher {
    private static final String API_KEY = "e9c72357429a44b3be25330cdabf9c1e";
    private static final String NEWS_API_URL = "https://newsapi.org/v2/top-headlines?country=us&apiKey=" + API_KEY;

    public static String fetchNewsJson() throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet request = new HttpGet(NEWS_API_URL);

        try (CloseableHttpResponse response = httpClient.execute(request)) {
            return EntityUtils.toString(response.getEntity());
        }
    }
}

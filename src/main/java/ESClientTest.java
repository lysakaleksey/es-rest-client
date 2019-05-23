import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ESClientTest {
	public static final String HEADER_CONTENT_TYPE = "Content-Type";
	public static final String MEDIA_TYPE_JSON = "application/json";

	public static final Header[] BULK_HEADERS =
		{
			new BasicHeader("User-Agent", "test-agent"),
			new BasicHeader("Accept", MEDIA_TYPE_JSON),
			new BasicHeader("Accept-Charset", "utf-8"),
			new BasicHeader(HEADER_CONTENT_TYPE, MEDIA_TYPE_JSON)
		};

	private final List<HttpHost> hosts;
	private final RestClient restClient;
	private final String host = "127.0.0.1";
	private final int port = 9200;
	private final String scheme = "http";
	private final int maxRetryTimeoutMillis = 500;  //Timeout to reproduce leak
	//private final int maxRetryTimeoutMillis = 200000; //Timeout to fix leak
	private final int poolSize = 32;
	private final ThreadPoolExecutor executor;
	private final Semaphore semaphore;

	public static void main(String[] args) {
		ESClientTest test = new ESClientTest();

		test.start();
	}

	public ESClientTest() {
		executor = new ThreadPoolExecutor(
			poolSize,
			poolSize,
			0L,
			TimeUnit.MILLISECONDS,
			new LinkedBlockingQueue<>());
		this.semaphore = new Semaphore(poolSize);

		final HttpHost[] httpHosts = new HttpHost[1];
		httpHosts[0] = new HttpHost(host, port, scheme);

		hosts = Collections.unmodifiableList(Arrays.asList(httpHosts));

		final RestClientBuilder restClientBuilder = RestClient.builder(httpHosts);

		restClientBuilder.setRequestConfigCallback(
			requestConfigBuilder -> requestConfigBuilder.setConnectionRequestTimeout(0));

		restClientBuilder.setMaxRetryTimeoutMillis(maxRetryTimeoutMillis);

		restClient = restClientBuilder.build();
	}

	private void submit(final Runnable runnable) throws InterruptedException {
		semaphore.acquire();
		try {
			executor.execute(() ->
			{
				try {
					runnable.run();
				} catch (final Exception e) {
					System.out.println("Caught exception: {}:" + e.toString());
				} finally {
					semaphore.release();
				}
			});
		} catch (final RejectedExecutionException e) {
			semaphore.release();
			throw e;
		}
	}

	private void request(final String requestBody) {
		try {

			final StringEntity entity = new StringEntity(requestBody, ContentType.APPLICATION_JSON);

			final Response response = restClient.performRequest("POST", "/_bulk", Collections.emptyMap(), entity, BULK_HEADERS);
			if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
				System.out.println("Error:" + response.getStatusLine().getStatusCode());
			}

		} catch (final Exception e) {
			System.out.println("Exception:" + e.toString());
		}
	}

	public void start() {
		StringBuilder builder = new StringBuilder();
		//query.josn file should have about 1MB - 2MB json content
		try (BufferedReader reader = new BufferedReader(new FileReader("query.json"))) {
			String line = reader.readLine();
			while (line != null) {
				builder.append(line);
				line = reader.readLine();
			}
		} catch (IOException ex) {

		}

		final String requestString = builder.toString();

		while (true) {
			try {
				submit(
					() ->
					{
						try {
							request(requestString);
						} catch (final Exception e) {
							System.out.println("Exception:" + e.toString());
						}
					});
			} catch (InterruptedException ex) {

			}
		}
	}
}
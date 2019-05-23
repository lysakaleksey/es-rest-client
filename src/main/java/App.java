import org.apache.http.HttpHost;
import org.elasticsearch.action.get.*;
import org.elasticsearch.client.*;

import java.io.IOException;
import java.util.*;
import java.util.function.Supplier;

public class App {
	public static void main(String[] args) throws IOException {
		String es = "http://192.168.1.31:9200";
		HttpHost[] hosts = Arrays.stream(es.split(","))
			.map(HttpHost::create)
			.toArray(HttpHost[]::new);

		RestClientBuilder builder = RestClient.builder(hosts);
		builder.setFailureListener(new RestClient.FailureListener() {
			public void onFailure(HttpHost host) {
				System.out.println("*** onFailure host " + host);
			}
		});
		int timeout = 60_000;
		builder.setMaxRetryTimeoutMillis(timeout);
		//builder.setRequestConfigCallback(rcb -> rcb.setSocketTimeout(timeout));

		RestHighLevelClient client = new RestHighLevelClient(builder);

		String indexName = "index";
		String typeName = "type";

		List<String> ids = Arrays.asList("0", "1", "2", "3", "4");

		Supplier<String> randomId = () -> ids.stream().skip((int) (ids.size() * Math.random())).findFirst().get();

		Runnable runnable = () -> {
			while (true) {
				String id = randomId.get();
				try {
					System.out.println("*** Id " + id + " trying to get it");
					GetRequest request = new GetRequest().index(indexName).type(typeName).id(id);
					GetResponse response = client.get(request);
					if (response.getSourceAsMap() == null) {
						System.out.println("No data: " + response);
					}
				} catch (Exception e) {
					System.out.println("*** Id " + id + " got exception: " + e.getMessage());
//					if (!e.getMessage().contains("listener timeout after waiting"))
				}
				try {
					Thread.sleep(1_000);
				} catch (InterruptedException ignore) {
				}
			}
		};

		for (int i = 0; i < 1; i++) {
			new Thread(runnable).start();
		}
	}
}

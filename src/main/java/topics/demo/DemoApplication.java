package topics.demo;

import javax.annotation.PostConstruct;

import com.azure.core.credential.TokenCredential;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.azure.identity.DefaultAzureCredentialBuilder;

import com.azure.messaging.eventhubs.*;
import com.azure.messaging.eventhubs.checkpointstore.blob.BlobCheckpointStore;
import com.azure.messaging.eventhubs.models.*;
import com.azure.storage.blob.*;


@SpringBootApplication
public class DemoApplication {
	@Value("${eventHubAddress}")
	private String eventHubAddress;
	@Value("${eventHubName}")
	private String eventHubName;
	@Value("${storageContainerConnection}")
	private String storageContainerConnection;


	public static void main(String[] args) {
		SpringApplication.run(DemoApplication.class, args);
	}


	@PostConstruct
	public void init() {
		TokenCredential credential = new DefaultAzureCredentialBuilder().build();

		BlobContainerAsyncClient blobContainerAsyncClient = new BlobContainerClientBuilder()
				.endpoint(storageContainerConnection)
				.credential(credential)
				.buildAsyncClient();

		EventProcessorClient client = new EventProcessorClientBuilder()
				.credential(eventHubAddress, eventHubName, credential)
				.checkpointStore(new BlobCheckpointStore(blobContainerAsyncClient))
				.consumerGroup(EventHubClientBuilder.DEFAULT_CONSUMER_GROUP_NAME)
				.processEvent(DemoApplication::processEvent)
				.processError(DemoApplication::processError)
				.buildEventProcessorClient();
		client.start();
	}

	public static void processEvent(EventContext context) {
		System.out.println("Processing event: " + context.getEventData().getBodyAsString());
		context.updateCheckpoint();
	}

	public static void processError(ErrorContext context) {
		System.out.println("Processing error: " + context.getThrowable());
	}



}

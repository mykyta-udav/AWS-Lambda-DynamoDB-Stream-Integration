package com.task06;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
// THIS IS THE CORRECT IMPORT FOR THE EVENT STREAM'S ATTRIBUTEVALUE CLASS
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;

import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.model.RetentionSetting;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(
		lambdaName = "audit_producer",
		roleName = "audit_producer-role",
		isPublishVersion = true,
		aliasName = "${lambdas_alias_name}",
		logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
		@EnvironmentVariable(key = "target_table", value = "${target_table}"),
		@EnvironmentVariable(key = "region", value = "${region}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {

	private final DynamoDbClient dynamoDbClient;
	private final String targetTable;

	public AuditProducer() {
		this.targetTable = System.getenv("target_table");
		String region = System.getenv("region");
		this.dynamoDbClient = DynamoDbClient.builder().region(Region.of(region)).build();
	}

	@Override
	public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
		for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
			if (record == null) {
				continue;
			}

			String eventName = record.getEventName();
			// These variables are now correctly typed to match the event's output
			Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
			Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();

			if ("INSERT".equals(eventName)) {
				processInsert(newImage);
			} else if ("MODIFY".equals(eventName)) {
				processModify(newImage, oldImage);
			}
		}
		return null;
	}

	private void processInsert(Map<String, AttributeValue> newImage) {
		String key = newImage.get("key").getS();

		// Use v2 AttributeValue for the PutRequest
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> auditItem = new HashMap<>();
		auditItem.put("id", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(UUID.randomUUID().toString()).build());
		auditItem.put("itemKey", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(key).build());
		auditItem.put("modificationTime", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(Instant.now().toString()).build());

		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> newValueMap = new HashMap<>();
		newValueMap.put("key", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(newImage.get("key").getS()).build());
		newValueMap.put("value", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(newImage.get("value").getN()).build());
		auditItem.put("newValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().m(newValueMap).build());

		putItemInAuditTable(auditItem);
	}

	private void processModify(Map<String, AttributeValue> newImage, Map<String, AttributeValue> oldImage) {
		String key = newImage.get("key").getS();

		// Use v2 AttributeValue for the PutRequest
		Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> auditItem = new HashMap<>();
		auditItem.put("id", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(UUID.randomUUID().toString()).build());
		auditItem.put("itemKey", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(key).build());
		auditItem.put("modificationTime", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(Instant.now().toString()).build());
		auditItem.put("updatedAttribute", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s("value").build());
		auditItem.put("oldValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(oldImage.get("value").getN()).build());
		auditItem.put("newValue", software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(newImage.get("value").getN()).build());

		putItemInAuditTable(auditItem);
	}

	private void putItemInAuditTable(Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> item) {
		PutItemRequest request = PutItemRequest.builder()
				.tableName(this.targetTable)
				.item(item)
				.build();
		dynamoDbClient.putItem(request);
	}
}
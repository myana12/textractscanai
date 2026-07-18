package org.example;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.fasterxml.jackson.databind.JsonNode;
import software.amazon.awssdk.regions.Region;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.services.textract.TextractClient;
import software.amazon.awssdk.services.textract.model.BlockType;
import software.amazon.awssdk.services.textract.model.DetectDocumentTextResponse;

import java.util.stream.Collectors;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main implements RequestHandler<SQSEvent,Void> {
//    @Override
//    public Void handleRequest(SQSEvent input, Context context) {
//        return null;
//    }

    private final TextractClient textractClient = TextractClient.builder()
            .region(Region.AP_SOUTH_1)
            .build();

    // Jackson's tool for reading JSON text into navigable objects.
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Void handleRequest(SQSEvent event, Context context) {

        // Even with batch size 1, SQS always sends a LIST of messages — loop through it.
        for (SQSEvent.SQSMessage message : event.getRecords()) {

            // getBody() = the raw JSON string, e.g.:
            // {"Records":[{"s3":{"bucket":{"name":"..."},"object":{"key":"..."}}}]}
            String body = message.getBody();
            context.getLogger().log("Raw SQS message body: " + body);

            try {
                // Parse the JSON string into a navigable tree structure.
                JsonNode root = objectMapper.readTree(body);

                // S3 event notifications wrap everything in a "Records" array.
                JsonNode s3Record = root.path("Records").get(0);

                // Drill down to the exact fields we need.
                String bucketName = s3Record.path("s3").path("bucket").path("name").asText();
                String objectKey = s3Record.path("s3").path("object").path("key").asText();

                context.getLogger().log("Bucket: " + bucketName + ", Key: " + objectKey);

                // Call Textract on that exact S3 object.
                String extractedText = extract(bucketName, objectKey);

                context.getLogger().log("Extracted text: " + extractedText);

            } catch (Exception e) {
                // If parsing/extraction fails, log it — don't let one bad message
                // crash silently. (We'll handle retries/DLQ properly later.)
                context.getLogger().log("Error processing message: " + e.getMessage());
            }
        }
        return null;
    }

    // Your existing Textract logic — unchanged, just given a home in this class.
    public String extract(String bucketName, String objectKey) {
        DetectDocumentTextResponse response = textractClient.detectDocumentText(request -> request
                .document(document -> document
                        .s3Object(s3Object -> s3Object
                                .bucket(bucketName)
                                .name(objectKey)
                                .build())
                        .build())
                .build());

        return transformTextDetectionResponse(response);
    }

    private String transformTextDetectionResponse(DetectDocumentTextResponse response) {
        return response.blocks()
                .stream()
                .filter(block -> block.blockType().equals(BlockType.LINE))
                .map(block -> block.text())
                .collect(Collectors.joining(" "));
    }

    //TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
        // to see how IntelliJ IDEA suggests fixing it.
//        IO.println(String.format("Hello and welcome!"));
//
//        for (int i = 1; i <= 5; i++) {
//            //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
//            // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
//            IO.println("i = " + i);
//        }


}

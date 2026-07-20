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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
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
//            String url=System.getenv("DB_URL");
//            String username=System.getenv("DB_USER");
//            String password=System.getenv("DB_PASSWORD");
//            try
//            {
//                Connection con = DriverManager.getConnection(url, username, password);
//            }
//            catch(SQLException e)
//            {
//                System.out.println("Connection failed");
//            }


            try {
                // Parse the JSON string into a navigable tree structure.
                JsonNode root = objectMapper.readTree(body);

                // S3 event notifications wrap everything in a "Records" array.
                JsonNode s3Record = root.path("Records").get(0);

                // Drill down to the exact fields we need.
                String bucketName = s3Record.path("s3").path("bucket").path("name").asText();
                String objectKey = s3Record.path("s3").path("object").path("key").asText();

                context.getLogger().log("Bucket: " + bucketName + ", Key: " + objectKey);


                String url=System.getenv("DB_URL");
                String username=System.getenv("DB_USER");
                String password=System.getenv("DB_PASSWORD");
                // Call Textract on that exact S3 object.
                String extractedText = extract(bucketName, objectKey);
                context.getLogger().log("Extracted text: " + extractedText);
                try (Connection con = DriverManager.getConnection(url, username, password)) {
                    String sql = "UPDATE document SET extracted_text = ?, status = ? WHERE path = ?";
                    PreparedStatement stmt = con.prepareStatement(sql);
                    stmt.setString(1, extractedText);
                    stmt.setString(2, "PROCESSED");
                    stmt.setString(3, objectKey);
                    stmt.executeUpdate();
                    context.getLogger().log("Database updated successfully");
                }
            } catch (Exception e) {
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

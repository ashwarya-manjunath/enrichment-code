package com.example.demo;

import java.io.IOException;
import java.security.Key;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jws;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.security.Keys;
import jakarta.annotation.PostConstruct;

@SpringBootApplication
@RestController
public class EnrichmentApplication implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentApplication.class);
    private final RestTemplate restTemplate = new RestTemplate();
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlockingQueue<Map<String, Object>> taskQueue = new LinkedBlockingQueue<>();
    private final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

    @Value("${WD_API_URL}")
    private String wdApiUrl;

    @Value("${WD_API_KEY}")
    private String wdApiKey;

    @Value("${WEBHOOK_SECRET}")
    private String webhookSecret;

    public static void main(String[] args) {
        SpringApplication.run(EnrichmentApplication.class, args);
    }

    @Override
    public void run(String... args) {
        logger.info("WD_API_URL: {}", wdApiUrl);
        logger.info("WEBHOOK_SECRET: {}", webhookSecret);
    }

    @PostConstruct
    public void init() {
        executor.setCorePoolSize(2);
        executor.setMaxPoolSize(5);
        executor.setQueueCapacity(10); // Avoid memory overflow
        executor.initialize();
        executor.execute(this::enrichmentWorker);
    }
    

    @PostMapping("/webhook")
    public ResponseEntity<Map<String, String>> webhook(@RequestHeader("Authorization") String authorizationHeader, @RequestBody String payload) {
        if (!validateJwt(authorizationHeader)) {
            return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body(Map.of("status", "unauthorized"));
        }

        try {
            JsonNode eventNode = objectMapper.readTree(payload);
            String event = eventNode.get("event").asText();
            logger.info("Received event: {}", event);

            if ("ping".equals(event)) {
                return ResponseEntity.ok(Map.of("status", "ok"));
            } else if ("enrichment.batch.created".equals(event)) {
                Map<String, Object> data = objectMapper.convertValue(eventNode, Map.class);
                taskQueue.add(data);
                return ResponseEntity.status(HttpStatus.ACCEPTED).body(Map.of("status", "accepted"));
            } else {
                return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(Map.of("status", "bad request"));
            }
        } catch (IOException e) {
            logger.error("Error processing webhook", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("status", "error"));
        }
    }
    private boolean validateJwt(String authorizationHeader) {
        try {
            if (authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
                logger.error("Invalid Authorization header format");
                return false;
            }
            String token = authorizationHeader.substring(7); // Extract token safely
            Key key = Keys.hmacShaKeyFor(Decoders.BASE64.decode(webhookSecret));
            Jws<Claims> claims = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(token);
            return claims.getBody() != null;
        } catch (Exception e) {
            logger.error("Invalid JWT", e);
            return false;
        }
    }
    

    private void enrichmentWorker() {
        while (true) {
            try {
                Map<String, Object> task = taskQueue.take();
                processEnrichment(task);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.error("Worker interrupted", e);
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void processEnrichment(Map<String, Object> task) {
        try {
            if (task == null || !task.containsKey("data")) {
                logger.warn("Task data is missing. Skipping enrichment.");
                return;
            }
    
            String version = (String) task.get("version");
            Map<String, Object> data = (Map<String, Object>) task.get("data");
    
            if (data == null) {
                logger.warn("Data object is null. Skipping processing.");
                return;
            }
    
            String projectId = (String) data.get("project_id");
            String collectionId = (String) data.get("collection_id");
            String batchId = (String) data.get("batch_id");
    
            if (projectId == null || collectionId == null || batchId == null) {
                logger.warn("Missing required fields in data. Skipping.");
                return;
            }
    
            String batchApi = String.format("%s/v2/projects/%s/collections/%s/batches/%s", wdApiUrl, projectId, collectionId, batchId);
            logger.info("Processing batch: {}", batchId);
    
            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(wdApiKey);
            headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
    
            ResponseEntity<String> response = restTemplate.exchange(batchApi, HttpMethod.GET, new HttpEntity<>(headers), String.class);
            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                List<Map<String, Object>> enrichedDocs = new ArrayList<>();
                for (String line : response.getBody().split("\n")) {
                    enrichedDocs.add(enrich(objectMapper.readTree(line)));
                }
                sendEnrichedData(batchApi, enrichedDocs, version);
            }
        } catch (Exception e) {
            logger.error("Error processing enrichment", e);
        }
    }
    
    private static final Map<String, String> ABBREVIATION_MAP = Map.of(
        "NY", "New York",
        "CA", "California",
        "TX", "Texas",
        "FL", "Florida",
        "IL", "Illinois"
        // Add more mappings as needed
    );
    
private Map<String, Object> enrich(JsonNode doc) {
    logger.info("Enriching document: {}", doc);

    if (doc == null || !doc.has("document_id")) {
        logger.warn("Invalid document format: Missing document_id.");
        return Collections.emptyMap();
    }

    // Create a map to store all fields from the input document
    Map<String, Object> enrichedDoc = objectMapper.convertValue(doc, Map.class);
    List<Map<String, Object>> featuresToSend = new ArrayList<>();

    if (doc.has("features")) {
        for (JsonNode feature : doc.get("features")) {
            JsonNode properties = feature.get("properties");
            if (properties != null && properties.has("field_name")) {
                String fieldName = properties.get("field_name").asText();
                
                // Replace abbreviation with full form if present in the map
                String mappedValue = ABBREVIATION_MAP.getOrDefault(fieldName, fieldName);

                Map<String, Object> annotation = Map.of(
                        "type", "annotation",
                        "properties", Map.of(
                                "type", "entities",
                                "entity_type", mappedValue, // Use mapped value
                                "entity_text", mappedValue,
                                "confidence", 1.0
                        )
                );
                featuresToSend.add(annotation);
            }
        }
    }

    // Update the "features" field with the enriched features
    enrichedDoc.put("features", featuresToSend);
    return enrichedDoc;
}


    private void sendEnrichedData(String batchApi, List<Map<String, Object>> enrichedDocs, String version) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setBearerAuth(wdApiKey);
            headers.setContentType(MediaType.APPLICATION_JSON);
    
            Map<String, Object> payload = Map.of(
                    "version", version,
                    "documents", enrichedDocs
            );
    
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(payload, headers);
            ResponseEntity<String> response = restTemplate.exchange(batchApi, HttpMethod.POST, request, String.class);
    
            if (response.getStatusCode().is2xxSuccessful()) {
                logger.info("Successfully sent enriched data.");
            } else {
                logger.error("Failed to send enriched data. Status: {}, Response: {}", 
                             response.getStatusCode(), response.getBody());
            }
        } catch (Exception e) {
            logger.error("Error sending enriched data", e);
        }
    }
    
}

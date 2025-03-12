package com.example.demo;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

@ExtendWith(MockitoExtension.class)
class EnrichmentApplicationTest {

    @InjectMocks
    private EnrichmentApplication enrichmentApplication;

    @Mock
    private RestTemplate restTemplate;

    @BeforeEach
    void setUp() {
        // Set up any necessary mock behavior here
    }

    @Test
    void testWebhookUnauthorized() {
        ResponseEntity<Map<String, String>> response = enrichmentApplication.webhook("InvalidToken", "{}");
        assertEquals(HttpStatus.UNAUTHORIZED, response.getStatusCode());
    }

    @Test
void testWebhookPingEvent() {
    String validToken = "Bearer validToken"; // Replace with a valid JWT for testing
    String payload = "{ \"event\": \"ping\" }";

    ResponseEntity<Map<String, String>> response = enrichmentApplication.webhook(validToken, payload);

    assertEquals(HttpStatus.OK, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("ok", response.getBody().get("status"));
}

    @Test
    void testWebhookEnrichmentEvent() {
        String validToken = "Bearer validToken"; // Replace with a valid JWT for testing
        String payload = "{ \"event\": \"enrichment.batch.created\", \"data\": { \"project_id\": \"123\", \"collection_id\": \"456\", \"batch_id\": \"789\" } }";

        ResponseEntity<Map<String, String>> response = enrichmentApplication.webhook(validToken, payload);

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("accepted", response.getBody().get("status"));
    }

}

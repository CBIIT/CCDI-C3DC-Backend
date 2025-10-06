package gov.nih.nci.bento_ri.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import gov.nih.nci.bento_ri.model.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.github.benmanes.caffeine.cache.Cache;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Service for making OAuth2 authenticated API calls to CPI (Child Participant Index) service
 */
@Service("CpiFetcherService")
public class CPIFetcherService {
    
    private static final Logger logger = LogManager.getLogger(CPIFetcherService.class);
    
    @Value("${cpi.oauth2.client.id:#{environment.OAUTH2_CLIENT_ID}}")
    private String clientId;
    
    @Value("${cpi.oauth2.client.secret:#{environment.OAUTH2_CLIENT_SECRET}}")
    private String clientSecret;
    
    @Value("${cpi.oauth2.token.uri:#{environment.OAUTH2_TOKEN_URI}}")
    private String tokenUri;
    
    @Value("${cpi.api.url:https://participantindex.ccdi.cancer.gov/v1/associated_participant_ids}")
    private String apiUrl;
    
    @Value("${cpi.domains.url:https://participantindex.ccdi.cancer.gov/v1/domains}")
    private String domainsUrl;
    
    @Value("${cpi.oauth2.scope:custom}")
    private String scope;
    
    private final ObjectMapper objectMapper;
    private final HttpClient httpClient;
    private final Cache<String, Object> cache;
    
    @Autowired
    public CPIFetcherService(Cache<String, Object> caffeineCache) {
        this.objectMapper = new ObjectMapper();
        this.httpClient = createHttpClient();
        this.cache = caffeineCache;
    }
    
    /**
     * Fetch associated participant IDs from CPI service with formatted response
     * 
     * @param participantRequests List of participant requests with participantId and studyId
     * @return Formatted response as List of FormattedCPIResponse
     * @throws Exception if the API call fails
     */
    public List<FormattedCPIResponse> fetchAssociatedParticipantIds(List<ParticipantRequest> participantRequests) throws Exception {
        logger.info("Fetching associated participant IDs for {} participants", participantRequests.size());
        
        if (participantRequests == null || participantRequests.isEmpty()) {
            return new ArrayList<>();
        }
        
        // Get access token
        String accessToken = getAccessToken();
        
        // Get domains information once
        Map<String, DomainInfo> domainsMap = fetchDomainsInfo(accessToken);
        
        // Transform all input requests to CPI format in a single request
        List<CPIParticipantRequest> cpiRequests = new ArrayList<>();
        for (ParticipantRequest participantRequest : participantRequests) {
            cpiRequests.add(new CPIParticipantRequest(participantRequest.getStudyId(), participantRequest.getParticipantId()));
        }
        CPIRequestBody requestBody = new CPIRequestBody(cpiRequests);
        
        // Make single API call for all participants
        Map<String, Object> apiResponse = makeApiCall(accessToken, requestBody);
        
        // Format the response for each participant
        List<FormattedCPIResponse> formattedResponses = new ArrayList<>();
        for (ParticipantRequest participantRequest : participantRequests) {
            FormattedCPIResponse formattedResponse = formatResponse(participantRequest, apiResponse, domainsMap);
            formattedResponses.add(formattedResponse);
        }
        
        return formattedResponses;
    }
    
    /**
     * Clear the domains cache - useful for testing or when domains are updated
     */
    public void clearDomainsCache() {
        cache.invalidate("cpi:domains");
        cache.invalidate("cpi:domains:count");
        logger.info("Cleared domains cache");
    }
    
    /**
     * Get OAuth2 access token using client credentials
     */
    private String getAccessToken() throws Exception {
        logger.debug("Requesting OAuth2 access token from: {}", tokenUri);
        
        if (clientId == null || clientSecret == null || tokenUri == null) {
            throw new IllegalStateException("OAuth2 configuration is missing. Please set OAUTH2_CLIENT_ID, OAUTH2_CLIENT_SECRET, and OAUTH2_TOKEN_URI environment variables.");
        }
        
        String credentials = clientId + ":" + clientSecret;
        String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
        
        String formData = "grant_type=client_credentials&scope=" + scope;
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUri))
                .header("Authorization", "Basic " + encodedCredentials)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(formData))
                .timeout(Duration.ofSeconds(30))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        if (response.statusCode() == 200) {
            OAuth2TokenResponse tokenResponse = objectMapper.readValue(response.body(), OAuth2TokenResponse.class);
            String accessToken = tokenResponse.getAccessToken();
            logger.debug("Successfully obtained access token");
            return accessToken;
        } else {
            logger.error("Failed to get access token: {} - {}", response.statusCode(), response.body());
            throw new Exception("Failed to get access token: " + response.statusCode() + " - " + response.body());
        }
    }
    
    /**
     * Fetch domains information from CPI service with caching
     */
    private Map<String, DomainInfo> fetchDomainsInfo(String accessToken) throws Exception {
        final String CACHE_KEY = "cpi:domains";
        final String COUNT_CACHE_KEY = "cpi:domains:count";
        
        // Check cache first
        @SuppressWarnings("unchecked")
        Map<String, DomainInfo> cachedDomains = (Map<String, DomainInfo>) cache.getIfPresent(CACHE_KEY);
        if (cachedDomains != null) {
            Integer domainCount = (Integer) cache.getIfPresent(COUNT_CACHE_KEY);
            String countInfo = domainCount != null ? domainCount.toString() : "unknown";
            logger.debug("Retrieved domains information from cache ({} domains)", countInfo);
            return cachedDomains;
        }
        
        // Cache miss - fetch from API
        logger.debug("Cache miss - fetching domains information from: {}", domainsUrl);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(domainsUrl))
                .header("Authorization", "Bearer " + accessToken)
                .header("Accept", "application/json")
                .GET()
                .timeout(Duration.ofSeconds(30))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        logger.debug("Domains API response status: {}", response.statusCode());
        
        if (response.statusCode() == 200) {
            DomainInfo[] domains = objectMapper.readValue(response.body(), DomainInfo[].class);
            Map<String, DomainInfo> domainsMap = new HashMap<>();
            for (DomainInfo domain : domains) {
                // Store both original case and uppercase for case-insensitive lookup
                domainsMap.put(domain.getDomainName(), domain);
                domainsMap.put(domain.getDomainName().toUpperCase(), domain);
                domainsMap.put(domain.getDomainName().toLowerCase(), domain);
            }
            
            // Cache both the domains map and the actual count
            cache.put(CACHE_KEY, domainsMap);
            cache.put(COUNT_CACHE_KEY, domains.length);
            logger.info("Successfully fetched and cached {} domains", domains.length);
            
            return domainsMap;
        } else {
            logger.error("Failed to fetch domains: {} - {}", response.statusCode(), response.body());
            throw new Exception("Failed to fetch domains: " + response.statusCode() + " - " + response.body());
        }
    }
    
    /**
     * Format the API response into the desired output format
     */
    @SuppressWarnings("unchecked")
    private FormattedCPIResponse formatResponse(ParticipantRequest originalRequest, Map<String, Object> apiResponse, Map<String, DomainInfo> domainsMap) {
        List<FormattedCPIResponse.CPIDataItem> cpiDataItems = new ArrayList<>();
        
        logger.debug("Formatting response for participant: {}, API response: {}", originalRequest.getParticipantId(), apiResponse);
        
        // Parse the correct API response structure
        if (apiResponse.containsKey("participant_ids")) {
            Object participantIds = apiResponse.get("participant_ids");
            if (participantIds instanceof List) {
                List<Map<String, Object>> participantList = (List<Map<String, Object>>) participantIds;
                
                for (Map<String, Object> participant : participantList) {
                    String participantId = (String) participant.get("participant_id");
                    String domainName = (String) participant.get("domain_name");
                    
                    // Check if this is the participant we're looking for
                    if (originalRequest.getParticipantId().equals(participantId) && 
                        originalRequest.getStudyId().equals(domainName)) {
                        
                        // Process the associated_ids for this participant
                        Object associatedIds = participant.get("associated_ids");
                        if (associatedIds instanceof List) {
                            List<Map<String, Object>> associatedList = (List<Map<String, Object>>) associatedIds;
                            
                            logger.debug("Found {} associated IDs for participant {}", associatedList.size(), participantId);
                            
                            for (Map<String, Object> associated : associatedList) {
                                String associatedParticipantId = (String) associated.get("participant_id");
                                String associatedDomainName = (String) associated.get("domain_name");
                                String domainCategory = (String) associated.get("domain_category");
                                
                                logger.debug("Processing associated participant: id={}, domain={}, category={}", 
                                            associatedParticipantId, associatedDomainName, domainCategory);
                                
                                // Get domain information with case-insensitive lookup
                                DomainInfo domainInfo = findDomainInfo(associatedDomainName, domainsMap);
                                
                                String domainDescription = domainInfo != null ? domainInfo.getDomainDescription() : "";
                                String dataLocation = domainInfo != null ? domainInfo.getDataLocation() : "";
                                
                                // If domain_category is null from API, try to get it from domain info
                                if (domainCategory == null && domainInfo != null) {
                                    domainCategory = domainInfo.getDomainCategory();
                                    logger.debug("Retrieved domain_category from domain info: {}", domainCategory);
                                }
                                
                                if (domainInfo == null) {
                                    logger.warn("No domain info found for domain: {} (tried case variations)", associatedDomainName);
                                }
                                
                                FormattedCPIResponse.CPIDataItem cpiDataItem = new FormattedCPIResponse.CPIDataItem(
                                    associatedParticipantId,    // associated_id (the actual associated participant)
                                    associatedDomainName,       // repository_of_synonym_id  
                                    domainDescription,          // domain_description
                                    domainCategory,             // domain_category
                                    dataLocation               // data_location
                                );
                                
                                cpiDataItems.add(cpiDataItem);
                            }
                        }
                        break; // Found our participant, no need to continue
                    }
                }
            }
        }
        
        // If no associated participants found, the participant has no associations
        if (cpiDataItems.isEmpty()) {
            logger.debug("No associated participants found for participant: {}", originalRequest.getParticipantId());
        }
        
        logger.info("Processed {} CPI data items for participant {}", cpiDataItems.size(), originalRequest.getParticipantId());
        return new FormattedCPIResponse(originalRequest.getParticipantId(), originalRequest.getStudyId(), cpiDataItems);
    }
    
    /**
     * Find domain info with case-insensitive lookup
     */
    private DomainInfo findDomainInfo(String domainName, Map<String, DomainInfo> domainsMap) {
        if (domainName == null) {
            return null;
        }
        
        // Try exact match first
        DomainInfo domainInfo = domainsMap.get(domainName);
        if (domainInfo != null) {
            return domainInfo;
        }
        
        // Try uppercase
        domainInfo = domainsMap.get(domainName.toUpperCase());
        if (domainInfo != null) {
            logger.debug("Found domain info using uppercase: {} -> {}", domainName, domainName.toUpperCase());
            return domainInfo;
        }
        
        // Try lowercase
        domainInfo = domainsMap.get(domainName.toLowerCase());
        if (domainInfo != null) {
            logger.debug("Found domain info using lowercase: {} -> {}", domainName, domainName.toLowerCase());
            return domainInfo;
        }
        
        return null;
    }
    
    /**
     * Make the actual API call to CPI service
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> makeApiCall(String accessToken, CPIRequestBody requestBody) throws Exception {
        logger.debug("Making API call to: {}", apiUrl);
        logger.debug("Request body: {}", requestBody);
        
        String jsonBody = objectMapper.writeValueAsString(requestBody);
        
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(apiUrl))
                .header("Authorization", "Bearer " + accessToken)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .method("GET", HttpRequest.BodyPublishers.ofString(jsonBody))
                .timeout(Duration.ofSeconds(30))
                .build();
        
        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        
        logger.debug("API response status: {}", response.statusCode());
        logger.debug("API response body: {}", response.body());
        
        if (response.statusCode() == 200) {
            Map<String, Object> responseMap = objectMapper.readValue(response.body(), Map.class);
            logger.info("Raw API response structure: {}", responseMap.keySet());
            logger.info("Full API response for debugging: {}", responseMap);
            return filterResponse(responseMap);
        } else {
            logger.error("API request failed: {} - {}", response.statusCode(), response.body());
            throw new Exception("API request failed: " + response.statusCode() + " - " + response.body());
        }
    }
    
    /**
     * Create HTTP client with default SSL verification
     */
    private HttpClient createHttpClient() {
        return HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(30))
                .build();
    }
    
    /**
     * Filter the response to remove supplementary_domains
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> filterResponse(Map<String, Object> response) {
        if (response == null) {
            return response;
        }
        
        // Create a copy of the response to avoid modifying the original
        Map<String, Object> filteredResponse = new HashMap<>(response);
        
        // Remove supplementary_domains from the top level
        filteredResponse.remove("supplementary_domains");
        
        // If there are participant data arrays, filter them too
        Object data = filteredResponse.get("data");
        if (data instanceof List) {
            List<Object> dataList = (List<Object>) data;
            for (Object item : dataList) {
                if (item instanceof Map) {
                    ((Map<String, Object>) item).remove("supplementary_domains");
                }
            }
        } else if (data instanceof Map) {
            ((Map<String, Object>) data).remove("supplementary_domains");
        }
        
        // Check for any nested structures that might contain supplementary_domains
        filteredResponse.entrySet().removeIf(entry -> 
            "supplementary_domains".equals(entry.getKey())
        );
        
        logger.debug("Filtered response, removed supplementary_domains");
        return filteredResponse;
    }
}

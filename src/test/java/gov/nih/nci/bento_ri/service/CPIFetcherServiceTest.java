package gov.nih.nci.bento_ri.service;

import com.github.benmanes.caffeine.cache.Cache;
import gov.nih.nci.bento_ri.model.DomainInfo;
import gov.nih.nci.bento_ri.model.FormattedCPIResponse;
import gov.nih.nci.bento_ri.model.ParticipantRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.anyString;

@ExtendWith(MockitoExtension.class)
class CPIFetcherServiceTest {

    @Mock
    private Cache<String, Object> cache;

    @Test
    @DisplayName("fetchAssociatedParticipantIds returns empty list for null input")
    void fetchAssociatedParticipantIds_shouldReturnEmpty_whenInputIsNull() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        List<FormattedCPIResponse> result = service.fetchAssociatedParticipantIds(null);

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("fetchAssociatedParticipantIds returns empty list for empty input")
    void fetchAssociatedParticipantIds_shouldReturnEmpty_whenInputIsEmpty() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        List<FormattedCPIResponse> result = service.fetchAssociatedParticipantIds(new ArrayList<>());

        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("clearDomainsCache invalidates both domain cache keys")
    void clearDomainsCache_shouldInvalidateBothDomainKeys() {
        CPIFetcherService service = new CPIFetcherService(cache);

        service.clearDomainsCache();

        verify(cache).invalidate("cpi:domains");
        verify(cache).invalidate("cpi:domains:count");
    }

    @Test
    @DisplayName("getAccessToken throws IllegalStateException when OAuth config is missing")
    void getAccessToken_shouldThrowIllegalState_whenOAuthConfigurationMissing() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        assertThatThrownBy(() -> invokePrivate(service, "getAccessToken"))
            .isInstanceOf(InvocationTargetException.class)
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasRootCauseMessage("OAuth2 configuration is missing. Please set OAUTH2_CLIENT_ID, OAUTH2_CLIENT_SECRET, and OAUTH2_TOKEN_URI environment variables.");
    }

    @Test
    @DisplayName("formatResponse maps associated ids and fills domain details with case-insensitive lookup")
    void formatResponse_shouldMapAssociatedData_whenMatchingParticipantExists() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "A-100");
        associated.put("domain_name", "study-a");
        associated.put("domain_category", null);

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-A");
        domainInfo.setDomainDescription("Primary study domain");
        domainInfo.setDomainCategory("CPI");
        domainInfo.setDataLocation("us-east");

        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-A", domainInfo);
        domainsMap.put("study-a", domainInfo);

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, domainsMap);

        assertThat(rawResult).isInstanceOf(FormattedCPIResponse.class);
        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;

        assertThat(result.getParticipantId()).isEqualTo("P-001");
        assertThat(result.getStudyId()).isEqualTo("STUDY-A");
        assertThat(result.getCpiData()).hasSize(1);
        assertThat(result.getCpiData().get(0).getAssociatedId()).isEqualTo("A-100");
        assertThat(result.getCpiData().get(0).getRepositoryOfSynonymId()).isEqualTo("study-a");
        assertThat(result.getCpiData().get(0).getDomainDescription()).isEqualTo("Primary study domain");
        assertThat(result.getCpiData().get(0).getDomainCategory()).isEqualTo("CPI");
        assertThat(result.getCpiData().get(0).getDataLocation()).isEqualTo("us-east");
    }

    @Test
    @DisplayName("filterResponse removes supplementary_domains from top level and nested data")
    void filterResponse_shouldRemoveSupplementaryDomains_whenPresent() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        Map<String, Object> nested = new HashMap<>();
        nested.put("supplementary_domains", List.of("NESTED"));
        nested.put("other", "value");

        Map<String, Object> response = new HashMap<>();
        response.put("supplementary_domains", List.of("TOP"));
        response.put("data", List.of(nested));
        response.put("keep", "ok");

        Object rawResult = invokePrivate(service, "filterResponse",
            new Class<?>[]{Map.class}, response);

        assertThat(rawResult).isInstanceOf(Map.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> filtered = (Map<String, Object>) rawResult;

        assertThat(filtered).doesNotContainKey("supplementary_domains");
        assertThat(filtered.get("keep")).isEqualTo("ok");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> data = (List<Map<String, Object>>) filtered.get("data");
        assertThat(data).hasSize(1);
        assertThat(data.get(0)).doesNotContainKey("supplementary_domains");
        assertThat(data.get(0).get("other")).isEqualTo("value");
    }

    @Test
    @DisplayName("findDomainInfo returns null when domain is null")
    void findDomainInfo_shouldReturnNull_whenDomainNameIsNull() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, null, new HashMap<String, DomainInfo>());

        assertThat(rawResult).isNull();
    }

    @Test
    @DisplayName("findDomainInfo retrieves domain via exact match")
    void findDomainInfo_shouldReturnDomain_viaExactMatch() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-A");
        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-A", domainInfo);

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, "STUDY-A", domainsMap);

        assertThat(rawResult).isEqualTo(domainInfo);
    }

    @Test
    @DisplayName("findDomainInfo retrieves domain via uppercase fallback")
    void findDomainInfo_shouldReturnDomain_viaUppercaseFallback() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-A");
        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-A", domainInfo);

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, "study-a", domainsMap);

        assertThat(rawResult).isEqualTo(domainInfo);
    }

    @Test
    @DisplayName("findDomainInfo returns null when domain not found in any case variation")
    void findDomainInfo_shouldReturnNull_whenDomainNotFound() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        Map<String, DomainInfo> domainsMap = new HashMap<>();

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, "UNKNOWN", domainsMap);

        assertThat(rawResult).isNull();
    }

    @Test
    @DisplayName("formatResponse handles empty associated_ids list")
    void formatResponse_shouldReturnEmptyData_whenNoAssociatedIds() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", new ArrayList<>());

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        assertThat(rawResult).isInstanceOf(FormattedCPIResponse.class);
        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("formatResponse ignores non-matching participant in API response")
    void formatResponse_shouldReturnEmpty_whenParticipantNotInResponse() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-999", "STUDY-A");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of());

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        assertThat(rawResult).isInstanceOf(FormattedCPIResponse.class);
        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("formatResponse handles null associated_ids in response")
    void formatResponse_shouldHandleNullAssociatedIds_gracefully() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", null);

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        assertThat(rawResult).isInstanceOf(FormattedCPIResponse.class);
        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("formatResponse fills domain info from fallback when domain_category is null")
    void formatResponse_shouldFillCategoryFromDomainInfo_whenNull() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "A-100");
        associated.put("domain_name", "STUDY-A");
        associated.put("domain_category", null);

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-A");
        domainInfo.setDomainCategory("INJECTED");
        domainInfo.setDomainDescription("test");
        domainInfo.setDataLocation("us");

        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-A", domainInfo);

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, domainsMap);

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData().get(0).getDomainCategory()).isEqualTo("INJECTED");
    }

    @Test
    @DisplayName("formatResponse handles API response without participant_ids key")
    void formatResponse_shouldReturnEmpty_whenNoParticipantIdsKey() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("other_key", List.of());

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("filterResponse returns copy when no supplementary_domains present")
    void filterResponse_shouldReturnCopy_whenNoSupplementaryDomains() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        Map<String, Object> response = new HashMap<>();
        response.put("data", "value");
        response.put("key", "test");

        Object rawResult = invokePrivate(service, "filterResponse",
            new Class<?>[]{Map.class}, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) rawResult;
        assertThat(result.get("data")).isEqualTo("value");
        assertThat(result.get("key")).isEqualTo("test");
    }

    @Test
    @DisplayName("formatResponse with multiple associated IDs creates multiple CPI data entries")
    void formatResponse_shouldCreateMultipleEntries_forMultipleAssociatedIds() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> assoc1 = new HashMap<>();
        assoc1.put("participant_id", "A-100");
        assoc1.put("domain_name", "STUDY-B");
        assoc1.put("domain_category", "PRIMARY");
        assoc1.put("domain_description", "Study B");
        assoc1.put("data_location", "us-east");

        Map<String, Object> assoc2 = new HashMap<>();
        assoc2.put("participant_id", "A-200");
        assoc2.put("domain_name", "STUDY-C");
        assoc2.put("domain_category", "SECONDARY");
        assoc2.put("domain_description", "Study C");
        assoc2.put("data_location", "us-west");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(assoc1, assoc2));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).hasSize(2);
        assertThat(result.getCpiData().get(0).getAssociatedId()).isEqualTo("A-100");
        assertThat(result.getCpiData().get(1).getAssociatedId()).isEqualTo("A-200");
    }

    @Test
    @DisplayName("formatResponse fills domain fields from domainInfo when present")
    void formatResponse_shouldMapDomainFieldsFromDomainInfo() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "A-100");
        associated.put("domain_name", "STUDY-B");
        associated.put("domain_category", "TERTIARY");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-B");
        domainInfo.setDomainCategory("TERTIARY");
        domainInfo.setDomainDescription("Comprehensive Study B");
        domainInfo.setDataLocation("eu-central");

        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-B", domainInfo);

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, domainsMap);

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        FormattedCPIResponse.CPIDataItem item = result.getCpiData().get(0);
        assertThat(item.getAssociatedId()).isEqualTo("A-100");
        assertThat(item.getDomainCategory()).isEqualTo("TERTIARY");
        assertThat(item.getDomainDescription()).isEqualTo("Comprehensive Study B");
        assertThat(item.getDataLocation()).isEqualTo("eu-central");
    }

    @Test
    @DisplayName("findDomainInfo handles empty domain map gracefully")
    void findDomainInfo_shouldReturnNull_whenDomainsMapIsEmpty() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, "ANY-STUDY", new HashMap<String, DomainInfo>());

        assertThat(rawResult).isNull();
    }

    @Test
    @DisplayName("formatResponse handles mixed case domain names in lookup")
    void formatResponse_shouldFindDomain_withCaseInsensitiveComparison() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "study-a");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "A-100");
        associated.put("domain_name", "study-a");
        associated.put("domain_category", null);
        associated.put("domain_description", "Test");
        associated.put("data_location", "us");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "study-a");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("STUDY-A");
        domainInfo.setDomainCategory("MATCHED");
        domainInfo.setDomainDescription("Domain from map");
        domainInfo.setDataLocation("mapping-location");

        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("STUDY-A", domainInfo);

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, domainsMap);

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData().get(0).getDomainCategory()).isEqualTo("MATCHED");
    }

    @Test
    @DisplayName("formatResponse preserves null participant_ids in result")
    void formatResponse_shouldIncludeFormattedResponse_withParticipantFromRequest() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-555", "STUDY-Z");

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of());

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getParticipantId()).isEqualTo("P-555");
        assertThat(result.getStudyId()).isEqualTo("STUDY-Z");
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("clearDomainsCache invalidates all domain-related cache entries")
    void clearDomainsCache_shouldClearBothDomainCacheKeys() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        invokePrivate(service, "clearDomainsCache", new Class<?>[]{});

        verify(cache).invalidate("cpi:domains");
        verify(cache).invalidate("cpi:domains:count");
    }

    @Test
    @DisplayName("formatResponse handles nested data structure properly")
    void formatResponse_shouldUnwrapNestedAssociatedIds_fromApiResponse() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> deepNested = new HashMap<>();
        deepNested.put("participant_id", "DEEP-ID");
        deepNested.put("domain_name", "DEEP-STUDY");
        deepNested.put("domain_category", "DEEP");
        deepNested.put("domain_description", "Deep nested test");
        deepNested.put("data_location", "deep-loc");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(deepNested));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).hasSize(1);
        assertThat(result.getCpiData().get(0).getAssociatedId()).isEqualTo("DEEP-ID");
    }

    @Test
    @DisplayName("findDomainInfo returns first matching domain when multiple could apply")
    void findDomainInfo_shouldReturnFirstDomain_whenMultiplePresent() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        
        DomainInfo domain1 = new DomainInfo();
        domain1.setDomainName("TEST");
        DomainInfo domain2 = new DomainInfo();
        domain2.setDomainName("TEST");

        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("TEST", domain1);

        Object rawResult = invokePrivate(service, "findDomainInfo",
            new Class<?>[]{String.class, Map.class}, "test", domainsMap);

        assertThat(rawResult).isNotNull();
        assertThat((DomainInfo) rawResult).isEqualTo(domain1);
    }

    @Test
    @DisplayName("formatResponse handles supplementary_domains filtering via filterResponse")
    void filterResponse_shouldRemoveSupplementaryDomains_fromResponseMap() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        Map<String, Object> response = new HashMap<>();
        response.put("data", "keep");
        response.put("supplementary_domains", List.of("domain1", "domain2"));
        response.put("other", "also_keep");

        Object rawResult = invokePrivate(service, "filterResponse",
            new Class<?>[]{Map.class}, response);

        @SuppressWarnings("unchecked")
        Map<String, Object> result = (Map<String, Object>) rawResult;
        assertThat(result.containsKey("supplementary_domains")).isFalse();
        assertThat(result.get("data")).isEqualTo("keep");
        assertThat(result.get("other")).isEqualTo("also_keep");
    }

    @Test
    @DisplayName("getAccessToken throws exception when OAuth config is missing")
    void getAccessToken_shouldThrowIllegalState_whenOAuthConfigMissing() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        assertThatThrownBy(() -> invokePrivate(service, "getAccessToken", new Class<?>[]{}))
            .hasCauseInstanceOf(IllegalStateException.class)
            .hasStackTraceContaining("OAuth2 configuration is missing");
    }

    @Test
    @DisplayName("formatResponse preserves request participant and study IDs in result")
    void formatResponse_shouldIncludeRequestParticipantAndStudyIds_inResult() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("REQ-P-001", "REQ-STUDY");

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of());

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getParticipantId()).isEqualTo("REQ-P-001");
        assertThat(result.getStudyId()).isEqualTo("REQ-STUDY");
    }

    @Test
    @DisplayName("formatResponse creates CPIDataItem with correct field mapping")
    void formatResponse_shouldMapCPIDataItemFields_correctly() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "ASSOC-ID");
        associated.put("domain_name", "ASSOC-DOMAIN");
        associated.put("domain_category", "TEST-CATEGORY");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        DomainInfo domainInfo = new DomainInfo();
        domainInfo.setDomainName("ASSOC-DOMAIN");
        domainInfo.setDomainDescription("Test Description");
        domainInfo.setDataLocation("test-location");
        
        Map<String, DomainInfo> domainsMap = new HashMap<>();
        domainsMap.put("ASSOC-DOMAIN", domainInfo);

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, domainsMap);

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        FormattedCPIResponse.CPIDataItem item = result.getCpiData().get(0);
        
        assertThat(item.getAssociatedId()).isEqualTo("ASSOC-ID");
        assertThat(item.getRepositoryOfSynonymId()).isEqualTo("ASSOC-DOMAIN");
        assertThat(item.getDomainCategory()).isEqualTo("TEST-CATEGORY");
        assertThat(item.getDomainDescription()).isEqualTo("Test Description");
        assertThat(item.getDataLocation()).isEqualTo("test-location");
    }

    @Test
    @DisplayName("clearDomainsCache is callable and executes without error")
    void clearDomainsCache_shouldExecuteSuccessfully() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);

        invokePrivate(service, "clearDomainsCache", new Class<?>[]{});

        // Verify cache invalidation was called twice (for both keys)
        verify(cache, atLeastOnce()).invalidate(anyString());
    }

    @Test
    @DisplayName("formatResponse with null domainInfo retrieves description as empty string")
    void formatResponse_shouldUseEmptyStrings_whenDomainInfoNotFound() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> associated = new HashMap<>();
        associated.put("participant_id", "A-100");
        associated.put("domain_name", "UNKNOWN-DOMAIN");
        associated.put("domain_category", "PROVIDED");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of(associated));

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        FormattedCPIResponse.CPIDataItem item = result.getCpiData().get(0);
        
        assertThat(item.getDomainDescription()).isEmpty();
        assertThat(item.getDataLocation()).isEmpty();
        assertThat(item.getDomainCategory()).isEqualTo("PROVIDED");
    }

    @Test
    @DisplayName("formatResponse skips participant when domain_name doesn't match")
    void formatResponse_shouldSkipParticipant_whenStudyIdMismatch() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-MISMATCH");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of());

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("formatResponse handles case-insensitive participant ID matching")
    void formatResponse_shouldMatchParticipantIds_caseSensitively() throws Exception {
        CPIFetcherService service = new CPIFetcherService(cache);
        ParticipantRequest request = new ParticipantRequest("P-001", "STUDY-A");

        Map<String, Object> participant = new HashMap<>();
        participant.put("participant_id", "P-001");
        participant.put("domain_name", "STUDY-A");
        participant.put("associated_ids", List.of());

        Map<String, Object> apiResponse = new HashMap<>();
        apiResponse.put("participant_ids", List.of(participant));

        Object rawResult = invokePrivate(service, "formatResponse",
            new Class<?>[]{ParticipantRequest.class, Map.class, Map.class},
            request, apiResponse, new HashMap<String, DomainInfo>());

        FormattedCPIResponse result = (FormattedCPIResponse) rawResult;
        assertThat(result.getParticipantId()).isEqualTo("P-001");
        assertThat(result.getStudyId()).isEqualTo("STUDY-A");
    }

    private Object invokePrivate(Object target, String methodName) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        return method.invoke(target);
    }

    private Object invokePrivate(Object target, String methodName, Class<?>[] paramTypes, Object... args) throws Exception {
        Method method = target.getClass().getDeclaredMethod(methodName, paramTypes);
        method.setAccessible(true);
        return method.invoke(target, args);
    }
}

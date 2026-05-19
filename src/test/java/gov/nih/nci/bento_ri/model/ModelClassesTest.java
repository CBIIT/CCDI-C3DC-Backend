package gov.nih.nci.bento_ri.model;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ModelClassesTest {

    @Test
    @DisplayName("DomainInfo getters return values set via setters")
    void domainInfo_shouldExposeMutableProperties_viaGettersAndSetters() {
        DomainInfo domainInfo = new DomainInfo();
        
        domainInfo.setDomainName("STUDY-A");
        domainInfo.setDomainDescription("Primary study");
        domainInfo.setStatus("active");
        domainInfo.setDomainCategory("CPI");
        domainInfo.setDataLocation("us-east");

        assertThat(domainInfo.getDomainName()).isEqualTo("STUDY-A");
        assertThat(domainInfo.getDomainDescription()).isEqualTo("Primary study");
        assertThat(domainInfo.getStatus()).isEqualTo("active");
        assertThat(domainInfo.getDomainCategory()).isEqualTo("CPI");
        assertThat(domainInfo.getDataLocation()).isEqualTo("us-east");
    }

    @Test
    @DisplayName("OAuth2TokenResponse constructor initializes fields")
    void oAuth2TokenResponse_shouldInitializeViaNoArgConstructor() {
        OAuth2TokenResponse response = new OAuth2TokenResponse();
        assertThat(response.getAccessToken()).isNull();
        assertThat(response.getTokenType()).isNull();
        assertThat(response.getExpiresIn()).isNull();
        assertThat(response.getScope()).isNull();
    }

    @Test
    @DisplayName("OAuth2TokenResponse getters and setters manage all fields")
    void oAuth2TokenResponse_shouldManageAllFields_viaSettersAndGetters() {
        OAuth2TokenResponse response = new OAuth2TokenResponse();
        
        response.setAccessToken("token-abc");
        response.setTokenType("Bearer");
        response.setExpiresIn(7200);
        response.setScope("read write");

        assertThat(response.getAccessToken()).isEqualTo("token-abc");
        assertThat(response.getTokenType()).isEqualTo("Bearer");
        assertThat(response.getExpiresIn()).isEqualTo(7200);
        assertThat(response.getScope()).isEqualTo("read write");
    }

    @Test
    @DisplayName("CPIRequestBody constructor accepts participant request list")
    void cpiRequestBody_shouldAcceptListOfParticipantRequests_inConstructor() {
        CPIParticipantRequest req1 = new CPIParticipantRequest("STUDY-A", "P-001");
        CPIParticipantRequest req2 = new CPIParticipantRequest("STUDY-B", "P-002");
        List<CPIParticipantRequest> requests = List.of(req1, req2);

        CPIRequestBody body = new CPIRequestBody(requests);

        assertThat(body.getParticipantIds()).hasSize(2);
        assertThat(body.getParticipantIds().get(0).getParticipantId()).isEqualTo("P-001");
        assertThat(body.getParticipantIds().get(1).getParticipantId()).isEqualTo("P-002");
    }

    @Test
    @DisplayName("CPIRequestBody no-arg constructor allows setting participants")
    void cpiRequestBody_shouldAllowSettingParticipantIds_viaSetters() {
        CPIRequestBody body = new CPIRequestBody();
        CPIParticipantRequest req = new CPIParticipantRequest("STUDY-A", "P-001");

        body.setParticipantIds(List.of(req));

        assertThat(body.getParticipantIds()).hasSize(1);
        assertThat(body.toString()).contains("participantIds");
    }

    @Test
    @DisplayName("CPIParticipantRequest constructor initializes domain and participant")
    void cpiParticipantRequest_shouldStoreConstructorArguments() {
        CPIParticipantRequest request = new CPIParticipantRequest("STUDY-X", "P-123");

        assertThat(request.getDomainName()).isEqualTo("STUDY-X");
        assertThat(request.getParticipantId()).isEqualTo("P-123");
        assertThat(request.toString()).contains("STUDY-X");
        assertThat(request.toString()).contains("P-123");
    }

    @Test
    @DisplayName("CPIParticipantRequest no-arg constructor and setters")
    void cpiParticipantRequest_shouldSupportSetters() {
        CPIParticipantRequest request = new CPIParticipantRequest();

        request.setDomainName("STUDY-Y");
        request.setParticipantId("P-456");

        assertThat(request.getDomainName()).isEqualTo("STUDY-Y");
        assertThat(request.getParticipantId()).isEqualTo("P-456");
    }

    @Test
    @DisplayName("ParticipantRequest constructor initializes participant and study")
    void participantRequest_shouldStoreConstructorArguments() {
        ParticipantRequest request = new ParticipantRequest("P-789", "STUDY-Z");

        assertThat(request.getParticipantId()).isEqualTo("P-789");
        assertThat(request.getStudyId()).isEqualTo("STUDY-Z");
        assertThat(request.toString()).contains("P-789");
        assertThat(request.toString()).contains("STUDY-Z");
    }

    @Test
    @DisplayName("ParticipantRequest no-arg constructor and setters")
    void participantRequest_shouldSupportSettersAndNoArgConstructor() {
        ParticipantRequest request = new ParticipantRequest();

        request.setParticipantId("P-111");
        request.setStudyId("STUDY-W");

        assertThat(request.getParticipantId()).isEqualTo("P-111");
        assertThat(request.getStudyId()).isEqualTo("STUDY-W");
    }

    @Test
    @DisplayName("FormattedCPIResponse constructor initializes all fields")
    void formattedCpiResponse_shouldStoreConstructorArguments() {
        FormattedCPIResponse.CPIDataItem item = new FormattedCPIResponse.CPIDataItem(
            "assoc-1", "repo-1", "desc-1", "cat-1", "loc-1"
        );
        FormattedCPIResponse response = new FormattedCPIResponse("P-555", "STUDY-C", List.of(item));

        assertThat(response.getParticipantId()).isEqualTo("P-555");
        assertThat(response.getStudyId()).isEqualTo("STUDY-C");
        assertThat(response.getCpiData()).hasSize(1);
    }

    @Test
    @DisplayName("FormattedCPIResponse setters manage participant and study ids")
    void formattedCpiResponse_shouldSupportSetters() {
        FormattedCPIResponse response = new FormattedCPIResponse();

        response.setParticipantId("P-666");
        response.setStudyId("STUDY-D");
        response.setCpiData(List.of());

        assertThat(response.getParticipantId()).isEqualTo("P-666");
        assertThat(response.getStudyId()).isEqualTo("STUDY-D");
        assertThat(response.getCpiData()).isEmpty();
    }

    @Test
    @DisplayName("CPIDataItem constructor initializes all fields")
    void cpiDataItem_shouldStoreAllConstructorArguments() {
        FormattedCPIResponse.CPIDataItem item = new FormattedCPIResponse.CPIDataItem(
            "assoc-id", "repo", "description", "category", "location"
        );

        assertThat(item.getAssociatedId()).isEqualTo("assoc-id");
        assertThat(item.getRepositoryOfSynonymId()).isEqualTo("repo");
        assertThat(item.getDomainDescription()).isEqualTo("description");
        assertThat(item.getDomainCategory()).isEqualTo("category");
        assertThat(item.getDataLocation()).isEqualTo("location");
    }

    @Test
    @DisplayName("CPIDataItem no-arg constructor and setters")
    void cpiDataItem_shouldSupportSettersAndNoArgConstructor() {
        FormattedCPIResponse.CPIDataItem item = new FormattedCPIResponse.CPIDataItem();

        item.setAssociatedId("a-id");
        item.setRepositoryOfSynonymId("r-id");
        item.setDomainDescription("desc");
        item.setDomainCategory("cat");
        item.setDataLocation("loc");

        assertThat(item.getAssociatedId()).isEqualTo("a-id");
        assertThat(item.getRepositoryOfSynonymId()).isEqualTo("r-id");
        assertThat(item.getDomainDescription()).isEqualTo("desc");
        assertThat(item.getDomainCategory()).isEqualTo("cat");
        assertThat(item.getDataLocation()).isEqualTo("loc");
    }

    @Test
    @DisplayName("CPIDataItem no-arg constructor initializes fields to null")
    void cpiDataItem_shouldInitializeFieldsToNull_viaNoArgConstructor() {
        FormattedCPIResponse.CPIDataItem item = new FormattedCPIResponse.CPIDataItem();

        assertThat(item.getAssociatedId()).isNull();
        assertThat(item.getRepositoryOfSynonymId()).isNull();
        assertThat(item.getDomainDescription()).isNull();
        assertThat(item.getDomainCategory()).isNull();
        assertThat(item.getDataLocation()).isNull();
    }

    @Test
    @DisplayName("FormattedCPIResponse no-arg constructor initializes fields to null")
    void formattedCpiResponse_shouldInitializeFieldsToNull_viaNoArgConstructor() {
        FormattedCPIResponse response = new FormattedCPIResponse();

        assertThat(response.getParticipantId()).isNull();
        assertThat(response.getStudyId()).isNull();
        assertThat(response.getCpiData()).isNull();
    }
}

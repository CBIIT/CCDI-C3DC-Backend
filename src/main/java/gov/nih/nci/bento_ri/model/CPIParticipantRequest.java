package gov.nih.nci.bento_ri.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data model for API request to CPI service
 */
public class CPIParticipantRequest {
    @JsonProperty("domain_name")
    private String domainName;
    
    @JsonProperty("participant_id")
    private String participantId;

    public CPIParticipantRequest() {}

    public CPIParticipantRequest(String domainName, String participantId) {
        this.domainName = domainName;
        this.participantId = participantId;
    }

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public String getParticipantId() {
        return participantId;
    }

    public void setParticipantId(String participantId) {
        this.participantId = participantId;
    }

    @Override
    public String toString() {
        return "CPIParticipantRequest{" +
                "domainName='" + domainName + '\'' +
                ", participantId='" + participantId + '\'' +
                '}';
    }
}

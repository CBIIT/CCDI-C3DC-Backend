package gov.nih.nci.bento_ri.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Data model for CPI API request body
 */
public class CPIRequestBody {
    @JsonProperty("participant_ids")
    private List<CPIParticipantRequest> participantIds;

    public CPIRequestBody() {}

    public CPIRequestBody(List<CPIParticipantRequest> participantIds) {
        this.participantIds = participantIds;
    }

    public List<CPIParticipantRequest> getParticipantIds() {
        return participantIds;
    }

    public void setParticipantIds(List<CPIParticipantRequest> participantIds) {
        this.participantIds = participantIds;
    }

    @Override
    public String toString() {
        return "CPIRequestBody{" +
                "participantIds=" + participantIds +
                '}';
    }
}

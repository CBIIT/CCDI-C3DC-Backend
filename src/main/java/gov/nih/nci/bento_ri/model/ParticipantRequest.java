package gov.nih.nci.bento_ri.model;

/**
 * Data model for participant request input
 */
public class ParticipantRequest {
    private String participantId;
    private String studyId;

    public ParticipantRequest() {}

    public ParticipantRequest(String participantId, String studyId) {
        this.participantId = participantId;
        this.studyId = studyId;
    }

    public String getParticipantId() {
        return participantId;
    }

    public void setParticipantId(String participantId) {
        this.participantId = participantId;
    }

    public String getStudyId() {
        return studyId;
    }

    public void setStudyId(String studyId) {
        this.studyId = studyId;
    }

    @Override
    public String toString() {
        return "ParticipantRequest{" +
                "participantId='" + participantId + '\'' +
                ", studyId='" + studyId + '\'' +
                '}';
    }
}

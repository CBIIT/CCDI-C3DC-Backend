package gov.nih.nci.bento_ri.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Data model for formatted CPI response
 */
public class FormattedCPIResponse {
    @JsonProperty("participant_id")
    private String participantId;
    
    @JsonProperty("study_id")
    private String studyId;
    
    @JsonProperty("cpi_data")
    private List<CPIDataItem> cpiData;

    public FormattedCPIResponse() {}

    public FormattedCPIResponse(String participantId, String studyId, List<CPIDataItem> cpiData) {
        this.participantId = participantId;
        this.studyId = studyId;
        this.cpiData = cpiData;
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

    public List<CPIDataItem> getCpiData() {
        return cpiData;
    }

    public void setCpiData(List<CPIDataItem> cpiData) {
        this.cpiData = cpiData;
    }

    /**
     * Nested class for CPI data items
     */
    public static class CPIDataItem {
        @JsonProperty("associated_id")
        private String associated_id;
        
        @JsonProperty("repository_of_synonym_id")
        private String repository_of_synonym_id;

        @JsonProperty("domain_description")
        private String domain_description;

        @JsonProperty("domain_category")
        private String domain_category;
        
        @JsonProperty("data_location")
        private String data_location;

        public CPIDataItem() {}

        public CPIDataItem(String associated_id, String repository_of_synonym_id, String domain_description,
                          String domain_category, String data_location) {
            this.associated_id = associated_id;
            this.repository_of_synonym_id = repository_of_synonym_id;
            this.domain_description = domain_description;
            this.domain_category = domain_category;
            this.data_location = data_location;
        }

        public String getAssociatedId() {
            return associated_id;
        }

        public void setAssociatedId(String associatedId) {
            this.associated_id = associatedId;
        }

        public String getRepositoryOfSynonymId() {
            return repository_of_synonym_id;
        }

        public void setRepositoryOfSynonymId(String repositoryOfSynonymId) {
            this.repository_of_synonym_id = repositoryOfSynonymId;
        }

        public String getDomainDescription() {
            return domain_description;
        }

        public void setDomainDescription(String domainDescription) {
            this.domain_description = domainDescription;
        }

        public String getDomainCategory() {
            return domain_category;
        }

        public void setDomainCategory(String domainCategory) {
            this.domain_category = domainCategory;
        }

        public String getDataLocation() {
            return data_location;
        }

        public void setDataLocation(String dataLocation) {
            this.data_location = dataLocation;
        }
    }
}

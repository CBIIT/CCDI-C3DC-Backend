package gov.nih.nci.bento_ri.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Data model for domain information from /v1/domains endpoint
 */
public class DomainInfo {
    @JsonProperty("domain_name")
    private String domainName;
    
    @JsonProperty("domain_description")
    private String domainDescription;
    
    @JsonProperty("status")
    private String status;
    
    @JsonProperty("domain_category")
    private String domainCategory;
    
    @JsonProperty("data_location")
    private String dataLocation;

    public DomainInfo() {}

    public String getDomainName() {
        return domainName;
    }

    public void setDomainName(String domainName) {
        this.domainName = domainName;
    }

    public String getDomainDescription() {
        return domainDescription;
    }

    public void setDomainDescription(String domainDescription) {
        this.domainDescription = domainDescription;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDomainCategory() {
        return domainCategory;
    }

    public void setDomainCategory(String domainCategory) {
        this.domainCategory = domainCategory;
    }

    public String getDataLocation() {
        return dataLocation;
    }

    public void setDataLocation(String dataLocation) {
        this.dataLocation = dataLocation;
    }
}

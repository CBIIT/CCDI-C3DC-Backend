# CPI Fetcher Service

## Overview

The CPI (Child Participant Index) Fetcher Service is a Java Spring Boot service that provides OAuth2 authenticated access to the CPI API. It allows you to fetch associated participant IDs by providing participant and study information.

## Features

- OAuth2 Client Credentials authentication
- Environment variable configuration
- RESTful API endpoint
- SSL verification disabled for development/testing
- Input validation and error handling
- Logging and monitoring
- Formatted response with domain information lookup
- Calls both `/v1/associated_participant_ids` and `/v1/domains` endpoints
- Combines data to provide comprehensive participant information

## Configuration

The service requires the following environment variables to be set:

### Required Environment Variables

```bash
OAUTH2_CLIENT_ID=your_oauth2_client_id
OAUTH2_CLIENT_SECRET=your_oauth2_client_secret  
OAUTH2_TOKEN_URI=https://nih-nci.okta.com/oauth2/ausqsuym0atpSxXTA297/v1/token
```

### Optional Configuration

You can also set these in `application.properties`:

```properties
# CPI Service Configuration
cpi.api.url=https://participantindex.ccdi.cancer.gov/v1/associated_participant_ids
cpi.domains.url=https://participantindex.ccdi.cancer.gov/v1/domains
cpi.oauth2.scope=custom
```

## API Usage

### REST Endpoint

**POST** `/api/v1/cpi/associated-participant-ids`

**Request Body:**
```json
[
  {
    "participantId": "COG_PAMUPE",
    "studyId": "pcdc"
  },
  {
    "participantId": "PARTICIPANT_123", 
    "studyId": "study_abc"
  }
]
```

**Response:**
```json
[
  {
    "participant_id": "COG_PAMUPE",
    "study_id": "pcdc",
    "cpi_data": [
      {
        "associated_id": "PARTICIPANT_123",
        "repository_of_synonym_id": "TARGET",
        "domain_description": "Therapeutically Applicable Research to Generate Effective Treatments",
        "domain_category": "study",
        "data_location": "https://www.cancer.gov/ccg/research/genome-sequencing/target"
      }
    ]
  }
]
```

### Health Check

**GET** `/api/v1/cpi/health`

**Response:**
```json
{
  "status": "UP",
  "service": "CPI Fetcher Service",
  "timestamp": 1691234567890
}
```

## Java Service Usage

You can also use the service directly in your Java code:

```java
@Autowired
private CPIFetcherService cpiFetcherService;

public void example() {
    List<ParticipantRequest> requests = Arrays.asList(
        new ParticipantRequest("COG_PAMUPE", "pcdc"),
        new ParticipantRequest("PARTICIPANT_123", "study_abc")
    );
    
    try {
        List<FormattedCPIResponse> response = cpiFetcherService.fetchAssociatedParticipantIds(requests);
        System.out.println("API Response: " + response);
    } catch (Exception e) {
        System.err.println("Error: " + e.getMessage());
    }
}
```

## Data Models

### ParticipantRequest
- `participantId` (String): The participant identifier
- `studyId` (String): The study/domain identifier

### FormattedCPIResponse
- `participant_id` (String): The original participant ID from input
- `study_id` (String): The original study ID from input  
- `cpi_data` (List): Array of CPI data items

### CPIDataItem (within cpi_data)
- `associated_id` (String): Associated participant ID from CPI
- `repository_of_synonym_id` (String): Domain name from CPI
- `domain_description` (String): Description from domains endpoint
- `domain_category` (String): Category from domains endpoint
- `data_location` (String): Data location URL from domains endpoint

### CPIParticipantRequest (Internal API format)
- `domain_name` (String): Maps to studyId
- `participant_id` (String): Maps to participantId

## Error Handling

The service provides comprehensive error handling for:

- Missing or invalid OAuth2 configuration
- Network connection issues
- Authentication failures
- API request failures
- Input validation errors

## Security

- OAuth2 Client Credentials flow for secure authentication
- SSL verification disabled for development (can be enabled for production)
- Sensitive credentials read from environment variables
- Access tokens are masked in logs

## Development Notes

1. **Environment Setup**: Ensure all required environment variables are set before running
2. **SSL Configuration**: The service currently disables SSL verification for development. Enable it for production use
3. **Logging**: Comprehensive logging is provided at DEBUG and INFO levels
4. **Testing**: Example test cases are provided in `CPIFetcherServiceTest.java`

## Files Created

- `gov.nih.nci.bento.model.ParticipantRequest` - Input data model
- `gov.nih.nci.bento.model.CPIParticipantRequest` - CPI API format model  
- `gov.nih.nci.bento.model.CPIRequestBody` - Request body wrapper
- `gov.nih.nci.bento.model.OAuth2TokenResponse` - OAuth2 token response model
- `gov.nih.nci.bento.service.CPIFetcherService` - Main service class
- `gov.nih.nci.bento.controller.CPIController` - REST controller
- `gov.nih.nci.bento.service.CPIFetcherServiceTest` - Example test class

## Example cURL Request

```bash
curl -X POST http://localhost:8080/api/v1/cpi/associated-participant-ids \
  -H "Content-Type: application/json" \
  -d '[
    {
      "participantId": "COG_PAMUPE",
      "studyId": "pcdc"
    }
  ]'
```

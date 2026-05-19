package gov.nih.nci.bento.utility;

import gov.nih.nci.bento_ri.model.OAuth2TokenResponse;
import gov.nih.nci.bento_ri.model.ParticipantRequest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class StrUtilTest {

	@Test
	@DisplayName("OAuth2TokenResponse toString masks access token value")
	void oAuth2TokenResponse_shouldMaskAccessToken_whenToStringCalled() {
		OAuth2TokenResponse tokenResponse = new OAuth2TokenResponse();
		tokenResponse.setAccessToken("super-secret-token");
		tokenResponse.setTokenType("Bearer");
		tokenResponse.setExpiresIn(3600);
		tokenResponse.setScope("read");

		String text = tokenResponse.toString();

		assertThat(text).contains("accessToken='***'");
		assertThat(text).contains("tokenType='Bearer'");
		assertThat(text).contains("expiresIn=3600");
		assertThat(text).contains("scope='read'");
		assertThat(text).doesNotContain("super-secret-token");
	}

	@Test
	@DisplayName("ParticipantRequest toString includes participant and study identifiers")
	void participantRequest_shouldIncludeIdentifiers_whenToStringCalled() {
		ParticipantRequest participantRequest = new ParticipantRequest("P-001", "STUDY-A");

		assertThat(participantRequest.getParticipantId()).isEqualTo("P-001");
		assertThat(participantRequest.getStudyId()).isEqualTo("STUDY-A");
		assertThat(participantRequest.toString()).contains("participantId='P-001'");
		assertThat(participantRequest.toString()).contains("studyId='STUDY-A'");
	}
}
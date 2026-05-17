package gov.nih.nci.bento;

import gov.nih.nci.bento_ri.model.CPIParticipantRequest;
import gov.nih.nci.bento_ri.model.CPIRequestBody;
import gov.nih.nci.bento_ri.model.FormattedCPIResponse;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class EsServiceTest {

	@Test
	@DisplayName("CPIRequestBody stores participant ids provided in constructor")
	void cpiRequestBody_shouldStoreParticipantIds_whenConstructed() {
		CPIParticipantRequest request = new CPIParticipantRequest("STUDY-A", "P-001");

		CPIRequestBody body = new CPIRequestBody(List.of(request));

		assertThat(body.getParticipantIds()).hasSize(1);
		assertThat(body.getParticipantIds().get(0).getDomainName()).isEqualTo("STUDY-A");
		assertThat(body.getParticipantIds().get(0).getParticipantId()).isEqualTo("P-001");
		assertThat(body.toString()).contains("participantIds");
	}

	@Test
	@DisplayName("FormattedCPIResponse nested CPIDataItem exposes expected getter values")
	void formattedCpiResponse_shouldExposeNestedItemData_whenConstructed() {
		FormattedCPIResponse.CPIDataItem dataItem = new FormattedCPIResponse.CPIDataItem(
			"A-100", "repo-A", "desc", "category", "location"
		);
		FormattedCPIResponse response = new FormattedCPIResponse("P-001", "STUDY-A", List.of(dataItem));

		assertThat(response.getParticipantId()).isEqualTo("P-001");
		assertThat(response.getStudyId()).isEqualTo("STUDY-A");
		assertThat(response.getCpiData()).hasSize(1);
		assertThat(response.getCpiData().get(0).getAssociatedId()).isEqualTo("A-100");
		assertThat(response.getCpiData().get(0).getRepositoryOfSynonymId()).isEqualTo("repo-A");
		assertThat(response.getCpiData().get(0).getDomainDescription()).isEqualTo("desc");
		assertThat(response.getCpiData().get(0).getDomainCategory()).isEqualTo("category");
		assertThat(response.getCpiData().get(0).getDataLocation()).isEqualTo("location");
	}
}

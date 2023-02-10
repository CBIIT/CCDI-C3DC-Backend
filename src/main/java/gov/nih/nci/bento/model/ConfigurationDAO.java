package gov.nih.nci.bento.model;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * The Configuration Bean, reads configuration setting from classpath:application.properties.
 */
@Configuration
@PropertySource("classpath:application.properties")
@RequiredArgsConstructor
@Getter
public class ConfigurationDAO {
	private static final Logger logger = LogManager.getLogger(ConfigurationDAO.class);

	//Bento API Version
	@Value("${bento.api.version}")
	private String bentoApiVersion;

	@Value("${graphql.es_schema}")
	private String esSchemaFile;

	//Operation Type Enable
	@Value("${allow_graphql_query}")
	private boolean allowGraphQLQuery;
	@Value("${allow_graphql_mutation}")
	private boolean allowGraphQLMutation;

	//Elasticsearch Configuration
	@Value("${es.host}")
	private String esHost;
	@Value("${es.port}")
	private int esPort;
	@Value("${es.scheme}")
	private String esScheme;
	@Value(("${es.filter.enabled}"))
	private boolean esFilterEnabled;
	@Value("${es.sign.requests:true}")
	private boolean esSignRequests;
	@Value("${es.service_name}")
	private String serviceName;
	@Value("${es.region}")
	private String region;
	//Testing
	@Value("${test.queries_file}")
	private String testQueriesFile;
}

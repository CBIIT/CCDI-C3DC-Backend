# Modules Index

This index is scoped to modules visible in the current repository snapshot.

| Module | Responsibility | Status |
|---|---|---|
| `src/main/java/gov/nih/nci/bento_ri/model` | GraphQL runtime wiring and result-shaping logic (private/public fetchers and CPI-related output models) | partial |
| `src/main/java/gov/nih/nci/bento_ri/service` | OpenSearch query/aggregation service, CPI API service, cache bean configuration | partial |
| `src/main/java/com/amazonaws/http` | AWS request-signing interceptor used for signed OpenSearch access | documented |
| `src/main/resources/graphql` | Private/public GraphQL schema contracts for query surface area | documented |
| `src/main/resources/yaml` | YAML files backing filter config and query resources | partial |
| `src/main/webapp/WEB-INF` | Web deployment descriptor and static welcome-file entrypoint metadata | documented |
| `src/test/java` | Limited tests focused on base `gov.nih.nci.bento` classes/utilities | partial |

## Scope Notes
- This bootstrap index does not yet include deep per-module traces.
- Base framework modules referenced as `gov.nih.nci.bento.*` are not present in this source subtree and remain partially documented.

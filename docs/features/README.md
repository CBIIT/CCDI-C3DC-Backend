# Features Index

This bootstrap index captures feature-level capabilities evident from GraphQL schemas and service code.

| Feature | Description | Status |
|---|---|---|
| Private GraphQL query surface | QueryType operations for participants, diagnoses, studies, survivals, treatments, cohort views, and counts | traced |
| Public GraphQL query surface | Public search/count GraphQL operations wired by `PublicESDataFetcher` | traced |
| OpenSearch facet filtering | Nested and range-aware filter construction and aggregation helpers in `InventoryESService` | traced |
| Cohort analytics | Cohort charts, KM plot, risk table, and cohort metadata/manifest resolvers | partial |
| CPI participant enrichment | Associated participant/domain lookup and response shaping via `CPIFetcherService` | traced |
| Startup cache warmup | Preload of `idsLists` cache in `@PostConstruct` hook | traced |
| Redis-backed caching behavior | Redis properties exist in config; direct use in RI classes not confirmed | partial |
| REST CPI endpoint exposure | Mentioned in docs, but controller class is not present in current source | not yet traced |

## Scope Notes
- Feature details are intentionally shallow in first-run bootstrap mode.
- Deep feature traces should be added only when requested for a specific flow.

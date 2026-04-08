#!/bin/bash
curl -X 'GET' \
  'http://localhost:9005/openapi/v3/entity/application?systemMetadata=false&includeSoftDelete=false&skipCache=false&aspects=testResults&aspects=structuredProperties&aspects=ownership&aspects=applicationKey&aspects=glossaryTerms&aspects=domains&aspects=applicationProperties&aspects=subTypes&aspects=institutionalMemory&aspects=globalTags&aspects=forms&aspects=status&pitKeepAlive=5m&count=10&sortCriteria=urn&sortOrder=ASCENDING&query=%2A' \
  -H 'accept: application/json' | jq .

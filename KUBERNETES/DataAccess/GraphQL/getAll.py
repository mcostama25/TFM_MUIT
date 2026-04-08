import requests
import json

GMS_URL = "http://localhost:8080"
TOKEN = "eyJhbGciOiJIUzI1NiJ9..."
PAGE_SIZE = 200

headers = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {TOKEN}"
}

QUERY = """
query getAllDatasets($start: Int!, $count: Int!) {
  searchAcrossEntities(input: {
    types: [DATASET],
    query: "*",
    start: $start,
    count: $count,
    facets: ["platform", "platformInstance", "origin"]
  }) {
    total
    start
    count
    facets {
      field
      aggregations { value count }
    }
    searchResults {
      entity {
        urn
        ... on Dataset {
          name
          platform { name properties { displayName } }
          dataPlatformInstance { instanceId }
          origin
          subTypes { typeNames }
          container { properties { name } }
          properties {
            description
            lastModified { time }
          }
          schemaMetadata {
            fieldCount
          }
          lastIngested
        }
      }
    }
  }
}
"""

def fetch_all_datasets():
    all_results = []
    start = 0
    total = None

    while total is None or start < total:
        response = requests.post(
            f"{GMS_URL}/api/graphql",
            headers=headers,
            json={"query": QUERY, "variables": {"start": start, "count": PAGE_SIZE}}
        )
        data = response.json()
        search = data["data"]["searchAcrossEntities"]

        if total is None:
            total = search["total"]
            print(f"Total datasets found: {total}")
            print("\n--- Platform Summary ---")
            for facet in search["facets"]:
                if facet["field"] == "platform":
                    for agg in facet["aggregations"]:
                        print(f"  {agg['value']}: {agg['count']} datasets")

        all_results.extend(search["searchResults"])
        start += PAGE_SIZE
        print(f"Fetched {min(start, total)}/{total}...")

    return all_results

results = fetch_all_datasets()

# Group by platform
from collections import defaultdict
by_platform = defaultdict(list)
for r in results:
    entity = r["entity"]
    platform = entity.get("platform", {}).get("name", "unknown")
    by_platform[platform].append(entity)

print("\n=== FULL CATALOG INVENTORY ===")
for platform, datasets in sorted(by_platform.items()):
    instance = datasets[0].get("dataPlatformInstance") or {}
    print(f"\n📦 {platform.upper()} ({instance.get('instanceId','')}) — {len(datasets)} datasets")
    for ds in datasets:
        container = (ds.get("container") or {}).get("properties", {}).get("name", "")
        fields = (ds.get("schemaMetadata") or {}).get("fieldCount", "?")
        print(f"   └─ {ds['name']}  [{fields} fields]  container: {container}")

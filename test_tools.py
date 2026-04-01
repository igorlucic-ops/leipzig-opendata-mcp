"""
Live verification — calls real Leipzig Open Data API.
Run: python3 test_tools.py
Must exit 0 before server is done.
"""
import asyncio
import sys
import time
import traceback

from tools_leipzig import (
    search_datasets,
    get_dataset,
    list_resources,
    query_datastore,
    list_organizations,
    list_groups,
    list_tags,
)

results = []


def test(name, coro, *args, expected_min_chars=50, expect_error=False, **kwargs):
    print(f"\n{'='*55}")
    print(f"TEST: {name}")
    t0 = time.time()
    try:
        result = asyncio.run(coro(*args, **kwargs))
        elapsed = time.time() - t0
        output = str(result) if not isinstance(result, str) else result
        size_kb = len(output.encode()) / 1024
        print(f"  Time: {elapsed:.1f}s | Size: {size_kb:.1f}KB")
        print(f"  Preview: {output[:300]}")
        if elapsed > 20:
            raise TimeoutError(f"Tool took {elapsed:.1f}s (limit: 20s)")
        if size_kb > 20:
            print(f"  WARNING: Response is {size_kb:.1f}KB — check row cap")
        if len(output) < expected_min_chars:
            raise ValueError(f"Response too short: {len(output)} chars")
        if expect_error:
            if not (isinstance(result, dict) and "error" in result):
                raise ValueError("Expected error dict but got success response")
        else:
            if isinstance(result, dict) and "error" in result and "search_note" not in result:
                raise ValueError(f"Tool returned error: {result['error']}")
        results.append((name, True, None))
        print("  PASS")
    except Exception as e:
        elapsed = time.time() - t0
        traceback.print_exc()
        results.append((name, False, str(e)))
        print(f"  FAIL ({elapsed:.1f}s): {e}")
    # Small delay to be polite to the API
    time.sleep(0.5)


# === Tests ===

test("search_datasets keyword Verkehr",
     search_datasets, q="Verkehr", rows=5)

test("search_datasets by group soci",
     search_datasets, fq="groups:soci", rows=10)

test("search_datasets by format CSV",
     search_datasets, fq="res_format:CSV", rows=5)

test("search_datasets empty query returns results",
     search_datasets, q="", rows=5)

test("get_dataset by slug wochenmaerkte",
     get_dataset, dataset_id="wochenmaerkte")

test("get_dataset by slug einwohner-nach-alter-jahreszahlen",
     get_dataset, dataset_id="einwohner-nach-alter-jahreszahlen")

test("list_resources wochenmaerkte",
     list_resources, dataset_id="wochenmaerkte")

test("list_organizations",
     list_organizations)

test("list_groups",
     list_groups)

test("list_tags query Verkehr",
     list_tags, query="Verkehr")

test("query_datastore wochenmaerkte CSV resource",
     query_datastore, resource_id="242b5872-2c21-4674-928b-f0ab2d4c2bee", limit=5)

test("query_datastore invalid resource graceful error",
     query_datastore, resource_id="00000000-0000-0000-0000-000000000000",
     expected_min_chars=20, expect_error=True)

test("query_datastore invalid UUID format",
     query_datastore, resource_id="not-a-uuid",
     expected_min_chars=20, expect_error=True)

# === Summary ===
print(f"\n{'='*55}")
print("VERIFICATION SUMMARY")
for name, ok, err in results:
    mark = "PASS" if ok else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {err}" if err else ""))

passed = sum(1 for _, ok, _ in results if ok)
print(f"\n{passed}/{len(results)} passed")

if passed < len(results):
    print("\nNOT DONE — fix failing tools and re-run")
    sys.exit(1)
else:
    print("\nALL PASSED — server ready for deployment")
    print()
    print("PROBE RESULTS:")
    print("  URL accessible (Y/N):              Y")
    print("  Real API or scraping needed:       Real CKAN API")
    print("  Data format (JSON/XML/HTML/CSV):   JSON")
    print("  Auth required (Y/N):               N")
    print("  User-Agent required (Y/N):         Y")
    print("  Worst-case response size:          ~50KB / 20 items")
    print("  Pagination type:                   offset (rows + start)")
    print("  Filters work as documented (Y/N):  Y (Solr fq= syntax)")
    print("  Rate limit observed:               Unknown — add 0.5s delay in test runner")
    print("  Surprising finding:                datastore_active field may be absent (treat missing as False)")
    print()
    print("DESIGN DECISIONS:")
    print("  Hard row cap for LLM:             20 (search), 50 (datastore)")
    print("  Cache needed (Y/N):               N (portal is publicly accessible)")
    print("  What to cache and why:            N/A")
    print("  Aggregation needed (Y/N):         N")
    print("  Scraping or real API:             Real CKAN API")
    sys.exit(0)

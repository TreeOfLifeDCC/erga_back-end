DATA_PORTAL_AGGREGATIONS = [
    "biosamples", "raw_data", "mapped_reads", "assemblies_status",
    "annotation_status", "annotation_complete", "project_name",
    "symbionts_assemblies_status", "symbionts_biosamples_status", "symbionts_raw_data_status"
    ,"metagenomes_assemblies_status","metagenomes_biosamples_status","metagenomes_raw_data_status","images_available"]


ARTICLES_AGGREGATIONS = ["pubYear", "journalTitle", "articleType"]

# Explicit allowlist of indices the public API may target. The `index` value
# comes straight from the URL path, so without this gate a caller could point
# the credentialed ES client at any index in the cluster (wildcards, _all,
# other apps' indices). Keep the substring-based branching in main.py — these
# are the only concrete index names that branching is allowed to resolve to.
ALLOWED_INDICES = frozenset({
    "data_portal", "data_portal_test",
    "tracking_status", "tracking_status_index_test",
    "articles", "articles_test",
    "summary",
    "nbn_atlas", "tol_qc",
})

PHYLOGENETIC_RANKS = (
        'kingdom', 'phylum', 'class', 'order', 'family', 'genus', 'species')
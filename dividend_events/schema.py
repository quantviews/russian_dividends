"""Versioned, explicit Parquet schemas. Monetary values are exact decimal strings."""

import polars as pl

SCHEMA_VERSION = "1.0.0"
PARSER_VERSION = "dividend-events/1.2.1"


def strings(names):
    return {name: pl.String for name in names.split()}


SCHEMAS = {
    "securities": strings("""security_id issuer_id ticker isin regnumber name share_class
        instrument_type jurisdiction currency depositary_ratio identity_status aliases_json
        source_ids_json origin_json listing_status mapping_status mapping_missing_observed_at"""),
    "events": strings("""event_id dividend_id revision_id supersedes_id security_id issuer_id
        ticker event_type status decision_body reporting_period change_type amount currency
        amount_basis amount_unknown_reason share_basis_date share_basis_text original_text
        decision_at published_at publication_precision publication_timezone observed_at effective_at
        public_available_at publication_evidence edition_evidence record_date record_date_origin
        last_buy_date last_buy_date_origin ex_date ex_date_origin calendar_version settlement_rule
        payment_from payment_to recipient_category actual_payment_date actual_payment_amount
        source_id source_url document_id raw_path sha256 parser_version evidence verification_status
        historical_availability gap_reason""") | {"partial_payment": pl.Boolean},
    "sources": strings("""source_id source_url document_id raw_path sha256 observed_at
        content_type http_status http_metadata_json source_kind parser_version"""),
    "coverage": strings("""security_id ticker status requested_from requested_to checked_at
        available_from available_to source_ids_json event_counts_json missing_fields_json reasons_json
        source_statuses_json"""),
    "conflicts": strings("""conflict_id security_id dividend_id event_ids_json field values_json
        status resolution evidence"""),
    "corporate_actions": strings("""action_id security_id ticker action_date kind ratio source_id
        verification_status basis_policy observed_at public_available_at"""),
    "reconciliation": strings("""security_id ticker csv_path csv_row record_date reporting_period
        csv_amount result event_ids_json reason"""),
    "classification": strings("""ticker instrument_type classification reason source_id"""),
}

EVENT_STATUSES = {
    "recommendation": {"proposed"}, "approval": {"approved"},
    "amendment": {"proposed", "approved"}, "cancellation": {"cancelled"},
    "no_dividend": {"no_dividend"}, "payment_terms": {"approved"},
    "payment": {"paid"}, "reference": {"unverified"},
}


def frame(table, rows):
    schema = SCHEMAS[table]
    return pl.DataFrame([{k: row.get(k) for k in schema} for row in rows], schema=schema)

import json
import logging
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as csv

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def handle_invalid_row(row):
    logger.warning(f"Invalid row detected: {row.text}")
    # Return "skip" to ignore the invalid row
    return "skip"


# File path
file_path = Path(
    "../../data/raw/2024_awards/FY2024_All_Contracts_Full_20250307_1.csv.zst"
).resolve()

schema = pa.schema(
    [
        ("contract_transaction_unique_key", pa.string()),
        ("contract_award_unique_key", pa.string()),
        ("award_id_piid", pa.string()),
        ("modification_number", pa.string()),
        ("transaction_number", pa.int64()),
        ("parent_award_agency_id", pa.string()),
        ("parent_award_agency_name", pa.string()),
        ("parent_award_id_piid", pa.string()),
        ("parent_award_modification_number", pa.string()),
        ("federal_action_obligation", pa.float64()),
        ("total_dollars_obligated", pa.float64()),
        ("total_outlayed_amount_for_overall_award", pa.float64()),
        ("base_and_exercised_options_value", pa.float64()),
        ("current_total_value_of_award", pa.float64()),
        ("base_and_all_options_value", pa.float64()),
        ("potential_total_value_of_award", pa.float64()),
        ("disaster_emergency_fund_codes_for_overall_award", pa.string()),
        ("outlayed_amount_from_COVID-19_supplementals_for_overall_award", pa.float64()),
        (
            "obligated_amount_from_COVID-19_supplementals_for_overall_award",
            pa.float64(),
        ),
        ("outlayed_amount_from_IIJA_supplemental_for_overall_award", pa.float64()),
        ("obligated_amount_from_IIJA_supplemental_for_overall_award", pa.float64()),
        ("action_date", pa.date32()),
        ("action_date_fiscal_year", pa.int64()),
        ("period_of_performance_start_date", pa.date32()),
        ("period_of_performance_current_end_date", pa.date32()),
        ("period_of_performance_potential_end_date", pa.timestamp("us")),
        ("ordering_period_end_date", pa.date32()),
        ("solicitation_date", pa.date32()),
        ("awarding_agency_code", pa.int64()),
        ("awarding_agency_name", pa.string()),
        ("awarding_sub_agency_code", pa.string()),
        ("awarding_sub_agency_name", pa.string()),
        ("awarding_office_code", pa.string()),
        ("awarding_office_name", pa.string()),
        ("funding_agency_code", pa.int64()),
        ("funding_agency_name", pa.string()),
        ("funding_sub_agency_code", pa.string()),
        ("funding_sub_agency_name", pa.string()),
        ("funding_office_code", pa.string()),
        ("funding_office_name", pa.string()),
        ("treasury_accounts_funding_this_award", pa.string()),
        ("federal_accounts_funding_this_award", pa.string()),
        ("object_classes_funding_this_award", pa.string()),
        ("program_activities_funding_this_award", pa.string()),
        ("foreign_funding", pa.string()),
        ("foreign_funding_description", pa.string()),
        ("sam_exception", pa.int64()),
        ("sam_exception_description", pa.string()),
        ("recipient_uei", pa.string()),
        ("recipient_duns", pa.string()),
        ("recipient_name", pa.string()),
        ("recipient_name_raw", pa.string()),
        ("recipient_doing_business_as_name", pa.string()),
        ("cage_code", pa.string()),
        ("recipient_parent_uei", pa.string()),
        ("recipient_parent_duns", pa.string()),
        ("recipient_parent_name", pa.string()),
        ("recipient_parent_name_raw", pa.string()),
        ("recipient_country_code", pa.string()),
        ("recipient_country_name", pa.string()),
        ("recipient_address_line_1", pa.string()),
        ("recipient_address_line_2", pa.string()),
        ("recipient_city_name", pa.string()),
        ("prime_award_transaction_recipient_county_fips_code", pa.int64()),
        ("recipient_county_name", pa.string()),
        ("prime_award_transaction_recipient_state_fips_code", pa.int64()),
        ("recipient_state_code", pa.string()),
        ("recipient_state_name", pa.string()),
        ("recipient_zip_4_code", pa.string()),
        ("prime_award_transaction_recipient_cd_original", pa.string()),
        ("prime_award_transaction_recipient_cd_current", pa.string()),
        ("recipient_phone_number", pa.string()),
        ("recipient_fax_number", pa.string()),
        ("primary_place_of_performance_country_code", pa.string()),
        ("primary_place_of_performance_country_name", pa.string()),
        ("primary_place_of_performance_city_name", pa.string()),
        ("prime_award_transaction_place_of_performance_county_fips_code", pa.int64()),
        ("primary_place_of_performance_county_name", pa.string()),
        ("prime_award_transaction_place_of_performance_state_fips_code", pa.int64()),
        ("primary_place_of_performance_state_code", pa.string()),
        ("primary_place_of_performance_state_name", pa.string()),
        ("primary_place_of_performance_zip_4", pa.string()),
        ("prime_award_transaction_place_of_performance_cd_original", pa.string()),
        ("prime_award_transaction_place_of_performance_cd_current", pa.string()),
        ("award_or_idv_flag", pa.string()),
        ("award_type_code", pa.string()),
        ("award_type", pa.string()),
        ("idv_type_code", pa.string()),
        ("idv_type", pa.string()),
        ("multiple_or_single_award_idv_code", pa.string()),
        ("multiple_or_single_award_idv", pa.string()),
        ("type_of_idc_code", pa.string()),
        ("type_of_idc", pa.string()),
        ("type_of_contract_pricing_code", pa.string()),
        ("type_of_contract_pricing", pa.string()),
        ("transaction_description", pa.string()),
        ("prime_award_base_transaction_description", pa.string()),
        ("action_type_code", pa.string()),
        ("action_type", pa.string()),
        ("solicitation_identifier", pa.string()),
        ("number_of_actions", pa.int64()),
        ("inherently_governmental_functions", pa.string()),
        ("inherently_governmental_functions_description", pa.string()),
        ("product_or_service_code", pa.string()),
        ("product_or_service_code_description", pa.string()),
        ("contract_bundling_code", pa.string()),
        ("contract_bundling", pa.string()),
        ("dod_claimant_program_code", pa.string()),
        ("dod_claimant_program_description", pa.string()),
        ("naics_code", pa.int64()),
        ("naics_description", pa.string()),
        ("recovered_materials_sustainability_code", pa.string()),
        ("recovered_materials_sustainability", pa.string()),
        ("domestic_or_foreign_entity_code", pa.string()),
        ("domestic_or_foreign_entity", pa.string()),
        ("dod_acquisition_program_code", pa.string()),
        ("dod_acquisition_program_description", pa.string()),
        ("information_technology_commercial_item_category_code", pa.string()),
        ("information_technology_commercial_item_category", pa.string()),
        ("epa_designated_product_code", pa.string()),
        ("epa_designated_product", pa.string()),
        ("country_of_product_or_service_origin_code", pa.string()),
        ("country_of_product_or_service_origin", pa.string()),
        ("place_of_manufacture_code", pa.string()),
        ("place_of_manufacture", pa.string()),
        ("subcontracting_plan_code", pa.string()),
        ("subcontracting_plan", pa.string()),
        ("extent_competed_code", pa.string()),
        ("extent_competed", pa.string()),
        ("solicitation_procedures_code", pa.string()),
        ("solicitation_procedures", pa.string()),
        ("type_of_set_aside_code", pa.string()),
        ("type_of_set_aside", pa.string()),
        ("evaluated_preference_code", pa.string()),
        ("evaluated_preference", pa.string()),
        ("research_code", pa.string()),
        ("research", pa.string()),
        ("fair_opportunity_limited_sources_code", pa.string()),
        ("fair_opportunity_limited_sources", pa.string()),
        ("other_than_full_and_open_competition_code", pa.string()),
        ("other_than_full_and_open_competition", pa.string()),
        ("number_of_offers_received", pa.int64()),
        ("commercial_item_acquisition_procedures_code", pa.string()),
        ("commercial_item_acquisition_procedures", pa.string()),
        ("small_business_competitiveness_demonstration_program", pa.string()),
        ("simplified_procedures_for_certain_commercial_items_code", pa.string()),
        ("simplified_procedures_for_certain_commercial_items", pa.string()),
        ("a76_fair_act_action_code", pa.string()),
        ("a76_fair_act_action", pa.string()),
        ("fed_biz_opps_code", pa.string()),
        ("fed_biz_opps", pa.string()),
        ("local_area_set_aside_code", pa.string()),
        ("local_area_set_aside", pa.string()),
        ("price_evaluation_adjustment_preference_percent_difference", pa.float64()),
        ("clinger_cohen_act_planning_code", pa.string()),
        ("clinger_cohen_act_planning", pa.string()),
        ("materials_supplies_articles_equipment_code", pa.string()),
        ("materials_supplies_articles_equipment", pa.string()),
        ("labor_standards_code", pa.string()),
        ("labor_standards", pa.string()),
        ("construction_wage_rate_requirements_code", pa.string()),
        ("construction_wage_rate_requirements", pa.string()),
        ("interagency_contracting_authority_code", pa.string()),
        ("interagency_contracting_authority", pa.string()),
        ("other_statutory_authority", pa.string()),
        ("program_acronym", pa.string()),
        ("parent_award_type_code", pa.string()),
        ("parent_award_type", pa.string()),
        ("parent_award_single_or_multiple_code", pa.string()),
        ("parent_award_single_or_multiple", pa.string()),
        ("major_program", pa.string()),
        ("national_interest_action_code", pa.string()),
        ("national_interest_action", pa.string()),
        ("cost_or_pricing_data_code", pa.string()),
        ("cost_or_pricing_data", pa.string()),
        ("cost_accounting_standards_clause_code", pa.string()),
        ("cost_accounting_standards_clause", pa.string()),
        ("government_furnished_property_code", pa.string()),
        ("government_furnished_property", pa.string()),
        ("sea_transportation_code", pa.string()),
        ("sea_transportation", pa.string()),
        ("undefinitized_action_code", pa.string()),
        ("undefinitized_action", pa.string()),
        ("consolidated_contract_code", pa.string()),
        ("consolidated_contract", pa.string()),
        ("performance_based_service_acquisition_code", pa.string()),
        ("performance_based_service_acquisition", pa.string()),
        ("multi_year_contract_code", pa.string()),
        ("multi_year_contract", pa.string()),
        ("contract_financing_code", pa.string()),
        ("contract_financing", pa.string()),
        ("purchase_card_as_payment_method_code", pa.string()),
        ("purchase_card_as_payment_method", pa.string()),
        ("contingency_humanitarian_or_peacekeeping_operation_code", pa.string()),
        ("contingency_humanitarian_or_peacekeeping_operation", pa.string()),
        ("alaskan_native_corporation_owned_firm", pa.string()),
        ("american_indian_owned_business", pa.string()),
        ("indian_tribe_federally_recognized", pa.string()),
        ("native_hawaiian_organization_owned_firm", pa.string()),
        ("tribally_owned_firm", pa.string()),
        ("veteran_owned_business", pa.string()),
        ("service_disabled_veteran_owned_business", pa.string()),
        ("woman_owned_business", pa.string()),
        ("women_owned_small_business", pa.string()),
        ("economically_disadvantaged_women_owned_small_business", pa.string()),
        ("joint_venture_women_owned_small_business", pa.string()),
        ("joint_venture_economic_disadvantaged_women_owned_small_bus", pa.string()),
        ("minority_owned_business", pa.string()),
        ("subcontinent_asian_asian_indian_american_owned_business", pa.string()),
        ("asian_pacific_american_owned_business", pa.string()),
        ("black_american_owned_business", pa.string()),
        ("hispanic_american_owned_business", pa.string()),
        ("native_american_owned_business", pa.string()),
        ("other_minority_owned_business", pa.string()),
        ("contracting_officers_determination_of_business_size", pa.string()),
        ("contracting_officers_determination_of_business_size_code", pa.string()),
        ("emerging_small_business", pa.string()),
        ("community_developed_corporation_owned_firm", pa.string()),
        ("labor_surplus_area_firm", pa.string()),
        ("us_federal_government", pa.string()),
        ("federally_funded_research_and_development_corp", pa.string()),
        ("federal_agency", pa.string()),
        ("us_state_government", pa.string()),
        ("us_local_government", pa.string()),
        ("city_local_government", pa.string()),
        ("county_local_government", pa.string()),
        ("inter_municipal_local_government", pa.string()),
        ("local_government_owned", pa.string()),
        ("municipality_local_government", pa.string()),
        ("school_district_local_government", pa.string()),
        ("township_local_government", pa.string()),
        ("us_tribal_government", pa.string()),
        ("foreign_government", pa.string()),
        ("organizational_type", pa.string()),
        ("corporate_entity_not_tax_exempt", pa.string()),
        ("corporate_entity_tax_exempt", pa.string()),
        ("partnership_or_limited_liability_partnership", pa.string()),
        ("sole_proprietorship", pa.string()),
        ("small_agricultural_cooperative", pa.string()),
        ("international_organization", pa.string()),
        ("us_government_entity", pa.string()),
        ("community_development_corporation", pa.string()),
        ("domestic_shelter", pa.string()),
        ("educational_institution", pa.string()),
        ("foundation", pa.string()),
        ("hospital_flag", pa.string()),
        ("manufacturer_of_goods", pa.string()),
        ("veterinary_hospital", pa.string()),
        ("hispanic_servicing_institution", pa.string()),
        ("receives_contracts", pa.string()),
        ("receives_financial_assistance", pa.string()),
        ("receives_contracts_and_financial_assistance", pa.string()),
        ("airport_authority", pa.string()),
        ("council_of_governments", pa.string()),
        ("housing_authorities_public_tribal", pa.string()),
        ("interstate_entity", pa.string()),
        ("planning_commission", pa.string()),
        ("port_authority", pa.string()),
        ("transit_authority", pa.string()),
        ("subchapter_scorporation", pa.string()),
        ("limited_liability_corporation", pa.string()),
        ("foreign_owned", pa.string()),
        ("for_profit_organization", pa.string()),
        ("nonprofit_organization", pa.string()),
        ("other_not_for_profit_organization", pa.string()),
        ("the_ability_one_program", pa.string()),
        ("private_university_or_college", pa.string()),
        ("state_controlled_institution_of_higher_learning", pa.string()),
        ("1862_land_grant_college", pa.string()),
        ("1890_land_grant_college", pa.string()),
        ("1994_land_grant_college", pa.string()),
        ("minority_institution", pa.string()),
        ("historically_black_college", pa.string()),
        ("tribal_college", pa.string()),
        ("alaskan_native_servicing_institution", pa.string()),
        ("native_hawaiian_servicing_institution", pa.string()),
        ("school_of_forestry", pa.string()),
        ("veterinary_college", pa.string()),
        ("dot_certified_disadvantage", pa.string()),
        ("self_certified_small_disadvantaged_business", pa.string()),
        ("small_disadvantaged_business", pa.string()),
        ("c8a_program_participant", pa.string()),
        ("historically_underutilized_business_zone_hubzone_firm", pa.string()),
        ("sba_certified_8a_joint_venture", pa.string()),
        ("highly_compensated_officer_1_name", pa.string()),
        ("highly_compensated_officer_1_amount", pa.float64()),
        ("highly_compensated_officer_2_name", pa.string()),
        ("highly_compensated_officer_2_amount", pa.float64()),
        ("highly_compensated_officer_3_name", pa.string()),
        ("highly_compensated_officer_3_amount", pa.float64()),
        ("highly_compensated_officer_4_name", pa.string()),
        ("highly_compensated_officer_4_amount", pa.float64()),
        ("highly_compensated_officer_5_name", pa.string()),
        ("highly_compensated_officer_5_amount", pa.float64()),
        ("usaspending_permalink", pa.string()),
        ("initial_report_date", pa.date32()),
        ("last_modified_date", pa.date32()),
    ]
)

json_file = Path(
    "D:/Projects/gov-data/usa_spending_data/src/data_dict/clean_key_to_domain_vals.json"
)
data_dict = json.loads(json_file.read_text())

# True values (from 'T' or 'Y' in exact matches)
true_columns = {
    str(v[key]).upper()
    for k, v in data_dict.items()
    if isinstance(v, dict)
    and (set(v.keys()) == {"F", "T"} or set(v.keys()) == {"Y", "N"})
    for key in v.keys()
    if key in ["T", "Y"]
}

# False values (from 'F' or 'N' in exact matches)
false_columns = {
    str(v[key]).upper()
    for k, v in data_dict.items()
    if isinstance(v, dict)
    and (set(v.keys()) == {"F", "T"} or set(v.keys()) == {"Y", "N"})
    for key in v.keys()
    if key in ["F", "N"]
}

new_fields = []
for field in schema:
    if field.name in data_dict and isinstance(data_dict[field.name], dict):
        values = set(data_dict[field.name].keys())
        if values == {"F", "T"} or values == {"Y", "N"}:
            new_type = pa.bool_()
        elif "reference" not in data_dict[field.name] and all(
            isinstance(k, str) and isinstance(v, str)
            for k, v in data_dict[field.name].items()
        ):
            new_type = pa.dictionary(pa.int32(), pa.string())
        else:
            new_type = field.type
    else:
        new_type = field.type
    new_fields.append(pa.field(field.name, new_type))

improved_schema = pa.schema(new_fields)

# Base true/false values
base_true_values = ["y", "Y", "T", "t", "YES", "Yes", "True", "TRUE"]
base_false_values = ["n", "N", "f", "F", "NO", "No", "False", "FALSE"]

# Combine base values with data_dict-derived values
combined_true_values = list(set(base_true_values) | true_columns)
combined_false_values = list(set(base_false_values) | false_columns)

convert_opts = csv.ConvertOptions(
    column_types=improved_schema,
    strings_can_be_null=True,
    true_values=combined_true_values,
    false_values=combined_false_values,
)

parse_opts = csv.ParseOptions(
    invalid_row_handler=handle_invalid_row,
)

try:
    table = csv.read_csv(
        file_path, parse_options=parse_opts, convert_options=convert_opts
    )
    logger.info("CSV %s successfully loaded", file_path)
except pa.ArrowInvalid:
    logger.exception("Failed to parse CSV", stack_info=True)
    raise

import os
import streamlit as st
import pandas as pd
from dotenv import load_dotenv
from functions import (
    merchant_report,
    cardholder_report,
    generate_prompt,
    get_gpt_analysis,
    client as bigquery_client
)
from fetch_data import fetch_query, test_query, fetch_combined_query

# Load environment variables
load_dotenv()

USER_ID = os.getenv("USER_ID")

def fetch_flagged_users():
    # Check if USER_ID is provided
    if USER_ID:
        # If USER_ID is provided, return a list with a single user dictionary
        return [{"user_id": int(USER_ID), "alert_type": "Custom Alert"}]
    else:
        # If no USER_ID, proceed with the usual query
        query = fetch_combined_query  # Use test_query if you're testing
        query_job = bigquery_client.query(query)
        results = query_job.result()
        return [dict(row) for row in results]

def fetch_betting_houses():
    bets_query = """
    SELECT * FROM `infinitepay-production.external_sources.betting_houses_document_numbers`
    """
    query_job = bigquery_client.query(bets_query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def fetch_pep_data(user_id):
    pep_query = rf"""
    DECLARE placeholder INT64 DEFAULT {user_id};

    WITH user_transactions AS (
        SELECT
            *,
            CASE
                WHEN debitor_user_id = placeholder THEN 'DEBIT'
                WHEN creditor_user_id = placeholder THEN 'CREDIT'
            END AS user_transaction_type,
            CASE
                WHEN debitor_user_id = placeholder THEN JSON_EXTRACT_SCALAR(credit_party_details, '$.document_number')
                WHEN creditor_user_id = placeholder THEN JSON_EXTRACT_SCALAR(debit_party_details, '$.document_number')
            END AS other_party_document_number
        FROM `maindb.pix_transfers`
        WHERE debitor_user_id = placeholder OR creditor_user_id = placeholder
    ),
    peps_aggregated AS (
        SELECT
            REGEXP_REPLACE(document_number, r'\D', '') AS document_number,
            name,
            job_description,
            STRING_AGG(DISTINCT agency, ', ') AS agencies
        FROM `infinitepay-production.maindb.politically_exposed_people`
        GROUP BY
            REGEXP_REPLACE(document_number, r'\D', ''),
            name,
            job_description
    )
    SELECT
        ut.user_transaction_type,
        peps.document_number AS pep_document_number,
        peps.name AS pep_name,
        peps.job_description AS job_description,
        peps.agencies,
        SUM(CAST(ut.amount AS FLOAT64) / 100) AS total_amount
    FROM user_transactions ut
    INNER JOIN peps_aggregated peps
        ON REGEXP_REPLACE(ut.other_party_document_number, r'\D', '') = peps.document_number
    GROUP BY
        peps.document_number,
        peps.name,
        ut.user_transaction_type,
        peps.job_description,
        peps.agencies
    ORDER BY
        peps.name,
        ut.user_transaction_type;
    """
    query_job = bigquery_client.query(pep_query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def analyze_user(user_data, betting_houses=None, pep_data=None):
    user_id = user_data['user_id']
    alert_type = user_data['alert_type']
    features = user_data.get('features') # This line will extract the 'features' column when available (AI Alert)

    # Determine if the user is a merchant or cardholder
    merchant_data = merchant_report(user_id, alert_type, pep_data=pep_data)
    if not merchant_data['merchant_info']:
        # If no merchant data, assume it's a cardholder
        report_data = cardholder_report(user_id, alert_type, pep_data=pep_data)
        user_type = "Cardholder"
    else:
        report_data = merchant_data
        user_type = "Merchant"

    report_data['user_id'] = user_id

    # Generate prompt for GPT analysis
    prompt = generate_prompt(report_data, user_type, alert_type, betting_houses=betting_houses, pep_data=pep_data, features=features)

    # Get GPT response
    gpt_analysis = get_gpt_analysis(prompt)

    return {
        "user_id": user_id,
        "user_type": user_type,
        "alert_type": alert_type,
        "features": features,
        "gpt_analysis": gpt_analysis
    }
    
def run_bot():
    flagged_users = fetch_flagged_users()
    betting_houses = fetch_betting_houses()
    results = []

    progress_bar = st.progress(0)
    status_text = st.empty()

    total_users = len(flagged_users)
    for i, user_data in enumerate(flagged_users):
        try:
            status_text.text(f"Analyzing user {user_data['user_id']}...")

            # Fetch pep_data for the current user
            pep_data = fetch_pep_data(user_data['user_id'])

            # Always pass betting_houses and pep_data, but only use them when needed
            analysis_result = analyze_user(user_data, betting_houses=betting_houses, pep_data=pep_data)

            # Display the GPT analysis result
            with st.expander(f"User ID: {analysis_result['user_id']} ({analysis_result['user_type']}) - Alert: {analysis_result['alert_type']}"):
                st.write("GPT Analysis:")
                st.markdown(analysis_result['gpt_analysis'])

            progress_bar.progress((i + 1) / total_users)
        except Exception as e:
            st.error(f"Error analyzing user {user_data['user_id']}: {str(e)}")

    status_text.text("Analysis complete!")
    return results

def main():
    st.title("AML Analysis Bot")

    if st.button("Run AML Analysis"):
        with st.spinner("Fetching flagged users..."):
            run_bot()

        st.success("Analysis complete!")

if __name__ == "__main__":
    main()
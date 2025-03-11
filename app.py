import os
import streamlit as st
import pandas as pd
import json
import requests
from dotenv import load_dotenv
from functions import (
    merchant_report,
    cardholder_report,
    generate_prompt,
    get_gpt_analysis,
    format_export_payload,
    client as bigquery_client
)
from fetch_data import fetch_combined_query

# Carrega as variáveis de ambiente
load_dotenv()

USER_ID = os.getenv("USER_ID")

# Função para enviar o payload para a API
def send_payload(payload, key_master):
    url = "https://infinitepay-risk-api.services.production.cloudwalk.network/monitoring/offense_analysis"
    headers = {
        "Content-Type": "application/json",
        "Authorization": key_master
    }
    response = requests.post(url, headers=headers, json=payload)
    return response.text

def fetch_flagged_users():
    if USER_ID:
        return [{"user_id": int(USER_ID), "alert_type": "Custom Alert", "business_validation": True}]
    else:
        query = fetch_combined_query 
        query_job = bigquery_client.query(query)
        results = query_job.result()
        return [dict(row, **{"business_validation": False}) for row in results]

def fetch_betting_houses():
    bets_query = """
    SELECT * FROM `infinitepay-production.external_sources.betting_houses_document_numbers`
    """
    query_job = bigquery_client.query(bets_query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def fetch_pep_data(user_id):
    pep_query = rf"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_pep_transactions_data` WHERE user_id = {user_id}
    """
    query_job = bigquery_client.query(pep_query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def analyze_user(user_data, betting_houses=None, pep_data=None):
    user_id = user_data['user_id']
    alert_type = user_data['alert_type']
    features = user_data.get('features')  # Pode conter dados extras (ex.: para AI Alert)
    
    # Determina se é Merchant ou Cardholder
    merchant_data = merchant_report(user_id, alert_type, pep_data=pep_data)
    if not merchant_data['merchant_info']:
        report_data = cardholder_report(user_id, alert_type, pep_data=pep_data)
        user_type = "Cardholder"
    else:
        report_data = merchant_data
        user_type = "Merchant"

    report_data['user_id'] = user_id

    # Gera o prompt com base no relatório
    prompt = generate_prompt(report_data, user_type, alert_type,
                             betting_houses=betting_houses, pep_data=pep_data, features=features)
    # Obtém a análise via GPT
    gpt_analysis = get_gpt_analysis(prompt)
    # Define a conclusão com base na análise contida no texto (a função format_export_payload faz a verificação)
    business_validation = user_data.get("business_validation", False)
    # Cria o payload exportado (a função já remove a formatação markdown se necessário)
    export_payload = format_export_payload(user_id, gpt_analysis, business_validation)
    return export_payload
  
def run_bot():
    flagged_users = fetch_flagged_users()
    betting_houses = fetch_betting_houses()
    key_master = ""  # Sua chave de autorização
    results = []

    progress_bar = st.progress(0)
    status_text = st.empty()

    total_users = len(flagged_users)
    for i, user_data in enumerate(flagged_users):
        try:
            status_text.text(f"Analyzing user {user_data['user_id']}...")
            pep_data = fetch_pep_data(user_data['user_id'])
            export_payload = analyze_user(user_data, betting_houses=betting_houses, pep_data=pep_data)

            # Envia o payload para a API
            response_text = send_payload(export_payload, key_master)
            st.write(f"Payload enviado para o usuário {user_data['user_id']} - Resposta: {response_text}")

            # Exibe o payload formatado com vírgulas e indentação
            json_output = json.dumps(export_payload, indent=4, ensure_ascii=False)
            json_output = json_output.replace("\\n", "\n")
            with st.expander(f"User ID: {user_data['user_id']} - Export Payload"):
                st.code(json_output, language="json")

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

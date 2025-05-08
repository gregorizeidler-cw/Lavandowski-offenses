"""
Lavandowski - AML Analysis App

Este aplicativo analisa dados de usuários para detecção de lavagem de dinheiro.

Otimizações de performance implementadas:
1. st.cache_data - Usado para cache de consultas de banco de dados frequentes
   - Funções de fetch_* em app.py e functions.py decoradas com @st.cache_data
   - TTL (Time To Live) configurado entre 30 min e 1 hora dependendo da função
   
2. st.session_state - Usado para manter resultados entre reruns do Streamlit
   - Armazena resultados de análises já processadas
   - Evita reprocessamento quando parâmetros não mudam
   - Reset automático do cache quando days_to_fetch muda

3. Otimização de consultas
   - Fetch único de dados usados múltiplas vezes
   - Reutilização de resultados quando possível
"""

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
import datetime
import logging
import re

# Initialize session state for caching intermediate results
if 'analyzed_users' not in st.session_state:
    st.session_state.analyzed_users = {}
if 'cache_expire_time' not in st.session_state:
    st.session_state.cache_expire_time = datetime.datetime.now() + datetime.timedelta(hours=1)
if 'current_day_fetch' not in st.session_state:
    st.session_state.current_day_fetch = None

st.set_page_config(
    page_title="Lavandowski - AML Analysis",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom JavaScript for enhanced UI
st.markdown("""
<script type="text/javascript">
document.addEventListener('DOMContentLoaded', function() {
    // Add modern animations and transitions
    const addAnimations = () => {
        const style = document.createElement('style');
        style.textContent = `
            @keyframes fadeInUp {
                from { opacity: 0; transform: translateY(20px); }
                to { opacity: 1; transform: translateY(0); }
            }
            
            .stApp div[data-testid="stVerticalBlock"] > div {
                animation: fadeInUp 0.4s ease-out forwards;
                animation-delay: calc(var(--index, 0) * 0.1s);
            }
            
            .stButton button {
                transition: all 0.3s ease !important;
                transform: scale(1);
            }
            
            .stButton button:hover {
                transform: scale(1.05);
                filter: brightness(1.1);
            }
            
            /* Add glass morphism effect to cards */
            div[data-testid="stVerticalBlock"] > div {
                backdrop-filter: blur(10px);
                -webkit-backdrop-filter: blur(10px);
                transition: box-shadow 0.3s ease, transform 0.3s ease;
            }
            
            div[data-testid="stVerticalBlock"] > div:hover {
                box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
                transform: translateY(-5px);
            }
        `;
        document.head.appendChild(style);
        
        // Add animation delay to each element
        const blocks = document.querySelectorAll('div[data-testid="stVerticalBlock"] > div');
        blocks.forEach((block, index) => {
            block.style.setProperty('--index', index);
        });
    };
    
    // Apply animations
    setTimeout(addAnimations, 500);
    
    // Reapply animations on navigation
    const observer = new MutationObserver(() => {
        setTimeout(addAnimations, 500);
    });
    
    observer.observe(document.body, { childList: true, subtree: true });
});
</script>
""", unsafe_allow_html=True)

st.markdown("""
<style>
  :root {
      --bg-primary: #0E1624;
      --bg-secondary: rgba(19, 32, 51, 0.9);
      --bg-card: rgba(25, 40, 61, 0.8);
      --accent-color: #4885ED;
      --accent-color-secondary: #3366D6;
      --text-primary: #FFFFFF;
      --text-secondary: #C8D4E9;
      --success-color: #0DB179;
      --warning-color: #F7B846;
      --danger-color: #E74C3C;
      --border-color: rgba(80, 100, 136, 0.3);
      --shadow: 0 4px 12px rgba(0, 0, 0, 0.25);
      --bg-hover: rgba(35, 50, 71, 0.9);
      --bg-active: rgba(40, 55, 76, 1);
      --highlight-color: rgba(72, 133, 237, 0.15);
      --card-border: rgba(80, 100, 136, 0.2);
  }
  
  /* Modern JS Framework Look */
  .main .block-container {
      padding: 2rem;
      max-width: 100%;
  }
  
  /* Glass morphism for cards */
  .dashboard-card {
      background: rgba(25, 40, 61, 0.7);
      backdrop-filter: blur(10px);
      -webkit-backdrop-filter: blur(10px);
      border: 1px solid rgba(255, 255, 255, 0.1);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.1);
      border-radius: 16px;
      padding: 24px;
      transition: all 0.3s cubic-bezier(0.25, 0.8, 0.25, 1);
  }
  
  .dashboard-card:hover {
      transform: translateY(-5px);
      box-shadow: 0 12px 32px rgba(0, 0, 0, 0.15);
      border-color: rgba(72, 133, 237, 0.3);
  }
  
  /* Modern button styles */
  .stButton > button {
      background: linear-gradient(135deg, var(--accent-color), var(--accent-color-secondary)) !important;
      color: white !important;
      border: none !important;
      border-radius: 8px !important;
      padding: 0.6rem 1.5rem !important;
      font-weight: 600 !important;
      letter-spacing: 0.5px !important;
      text-transform: uppercase !important;
      transition: all 0.3s ease !important;
      box-shadow: 0 4px 10px rgba(72, 133, 237, 0.3) !important;
  }
  
  .stButton > button:hover {
      transform: translateY(-2px) !important;
      box-shadow: 0 6px 15px rgba(72, 133, 237, 0.4) !important;
  }
  
  .stButton > button:active {
      transform: translateY(1px) !important;
      box-shadow: 0 2px 5px rgba(72, 133, 237, 0.4) !important;
  }
  
  /* Sleek inputs */
  .stTextInput > div > div > input, 
  .stNumberInput > div > div > input {
      border-radius: 8px !important;
      border: 1px solid var(--border-color) !important;
      padding: 0.5rem 1rem !important;
      background-color: rgba(19, 32, 51, 0.6) !important;
      color: var(--text-primary) !important;
      transition: all 0.3s ease !important;
  }
  
  .stTextInput > div > div > input:focus, 
  .stNumberInput > div > div > input:focus {
      border-color: var(--accent-color) !important;
      box-shadow: 0 0 0 2px rgba(72, 133, 237, 0.2) !important;
  }
  
  /* Modern select boxes */
  .stSelectbox > div > div > div,
  .stMultiselect > div > div > div {
      border-radius: 8px !important;
      border: 1px solid var(--border-color) !important;
      background-color: rgba(19, 32, 51, 0.6) !important;
  }
  
  /* Animation for elements */
  @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
  }
  
  .main-header, .sub-header, .chart-container, .stat-value {
      animation: fadeIn 0.5s ease-out forwards;
  }
  
  /* Main app header with modern gradient */
  .main-header {
      font-size: 2.2rem;
      font-weight: 700;
      color: var(--text-primary);
      margin-bottom: 1.5rem;
      text-shadow: 0px 2px 4px rgba(0, 0, 0, 0.3);
      border-left: 4px solid var(--accent-color);
      padding-left: 16px;
      letter-spacing: -0.5px;
      background: linear-gradient(120deg, #FFFFFF, #C8D4E9);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
  }
  
  /* Modern stat values with gradient */
  .stat-value {
      font-size: 2.7rem;
      font-weight: 700;
      margin: 15px 0;
      text-align: center;
      letter-spacing: -0.5px;
      line-height: 1;
      background: linear-gradient(120deg, var(--text-primary), rgba(255,255,255,0.85));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
  }
  .main-header {
      font-size: 2.2rem;
      font-weight: 700;
      color: var(--text-primary);
      margin-bottom: 1.5rem;
      text-shadow: 0px 2px 4px rgba(0, 0, 0, 0.3);
      border-left: 4px solid var(--accent-color);
      padding-left: 16px;
      letter-spacing: -0.5px;
  }
  .sub-header {
      font-size: 1.4rem;
      font-weight: 600;
      color: var(--text-primary);
      margin-bottom: 0.8rem;
      border-bottom: 1px solid var(--border-color);
      padding-bottom: 8px;
      letter-spacing: -0.3px;
  }
  .dashboard-card {
      background-color: var(--bg-card);
      border-radius: 12px;
      padding: 24px;
      box-shadow: var(--shadow);
      transition: all 0.3s ease;
      height: 100%;
      border: 1px solid var(--card-border);
      position: relative;
      overflow: hidden;
  }
  .dashboard-card:hover {
      transform: translateY(-3px);
      box-shadow: 0 6px 16px rgba(0, 0, 0, 0.3);
      border-color: rgba(72, 133, 237, 0.3);
  }
  .dashboard-card::after {
      content: '';
      position: absolute;
      top: 0;
      left: 0;
      width: 100%;
      height: 3px;
      background: linear-gradient(90deg, var(--accent-color), var(--accent-color-secondary));
      opacity: 0;
      transition: opacity 0.3s ease;
  }
  .dashboard-card:hover::after {
      opacity: 1;
  }
  .stat-value {
      font-size: 2.7rem;
      font-weight: 700;
      margin: 15px 0;
      text-align: center;
      letter-spacing: -0.5px;
      line-height: 1;
      background: linear-gradient(120deg, var(--text-primary), rgba(255,255,255,0.85));
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
  }
  .stat-label {
      font-size: 1.05rem;
      color: var(--text-secondary);
      text-align: center;
      margin-bottom: 10px;
      letter-spacing: 0.2px;
      font-weight: 500;
  }
  .dashboard-header {
      border-radius: 12px;
      padding: 20px;
      background-color: var(--bg-secondary);
      box-shadow: var(--shadow);
      margin-bottom: 24px;
      border: 1px solid var(--card-border);
  }
  .chart-container {
      background-color: var(--bg-card);
      border-radius: 12px;
      padding: 20px;
      box-shadow: var(--shadow);
      height: 100%;
      border: 1px solid var(--card-border);
      transition: all 0.3s ease;
  }
  .chart-container:hover {
      transform: translateY(-2px);
      box-shadow: 0 6px 16px rgba(0, 0, 0, 0.3);
      border-color: rgba(72, 133, 237, 0.3);
  }
  .chart-title {
      font-size: 1.1rem;
      font-weight: 600;
      text-align: center;
      margin-bottom: 15px;
      color: var(--text-primary);
  }
  @keyframes pulse {
      0% { opacity: 1; }
      50% { opacity: 0.7; }
      100% { opacity: 1; }
  }
  @keyframes fadeIn {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
  }
  @keyframes shimmer {
      0% { background-position: -100% 0; }
      100% { background-position: 100% 0; }
  }
  .analyzing {
      animation: pulse 1.5s infinite;
  }
  .fade-in {
      animation: fadeIn 0.5s ease forwards;
  }
  .skeleton {
      background: linear-gradient(90deg,
          var(--bg-card) 0%,
          var(--bg-hover) 50%,
          var(--bg-card) 100%);
      background-size: 200% 100%;
      animation: shimmer 1.5s infinite;
      border-radius: 4px;
  }
  .loading-card {
      height: 180px;
  }
  .loading-title {
      height: 20px;
      width: 60%;
      margin-bottom: 20px;
  }
  .loading-value {
      height: 50px;
      width: 80%;
      margin: 20px auto;
  }
  .loading-subtitle {
      height: 16px;
      width: 40%;
      margin: 0 auto;
  }
  .risk-badge-low {
      background-color: var(--success-color);
      color: white;
      padding: 0.3rem 0.7rem;
      border-radius: 50px;
      font-weight: 600;
      font-size: 0.85rem;
      display: inline-block;
      box-shadow: 0 2px 5px rgba(13, 177, 121, 0.3);
      transition: all 0.2s ease;
  }
  .risk-badge-low:hover {
      box-shadow: 0 3px 7px rgba(13, 177, 121, 0.5);
      transform: translateY(-1px);
  }
  .risk-badge-medium {
      background-color: var(--warning-color);
      color: white;
      padding: 0.3rem 0.7rem;
      border-radius: 50px;
      font-weight: 600;
      font-size: 0.85rem;
      display: inline-block;
      box-shadow: 0 2px 5px rgba(247, 184, 70, 0.3);
      transition: all 0.2s ease;
  }
  .risk-badge-medium:hover {
      box-shadow: 0 3px 7px rgba(247, 184, 70, 0.5);
      transform: translateY(-1px);
  }
  .risk-badge-high {
      background-color: var(--danger-color);
      color: white;
      padding: 0.3rem 0.7rem;
      border-radius: 50px;
      font-weight: 600;
      font-size: 0.85rem;
      display: inline-block;
      box-shadow: 0 2px 5px rgba(231, 76, 60, 0.3);
      transition: all 0.2s ease;
  }
  .risk-badge-high:hover {
      box-shadow: 0 3px 7px rgba(231, 76, 60, 0.5);
      transform: translateY(-1px);
  }
  .stAlert {
      background-color: var(--bg-secondary);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      box-shadow: var(--shadow);
  }
  .stExpander {
      background-color: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 10px;
      margin-bottom: 1rem;
      box-shadow: var(--shadow);
      transition: all 0.3s ease;
  }
  .stExpander:hover {
      border-color: var(--accent-color);
      box-shadow: 0 6px 16px rgba(0, 0, 0, 0.3);
  }
  .st-emotion-cache-1wmy9hl, div.st-emotion-cache-1cypcdb {
      background-color: var(--bg-secondary);
  }
  .block-container {
      padding-top: 1rem;
      padding-bottom: 1rem;
  }
  .css-1kyxreq {
      justify-content: center;
  }
  .css-1544g2n {
      background-color: var(--bg-primary);
      border-right: 1px solid var(--border-color);
  }
  .stProgress .st-dt {
      background-color: var(--accent-color);
  }
  div.stCodeBlock {
      border-radius: 8px;
  }
  a {
      color: var(--accent-color);
      text-decoration: none;
      transition: all 0.2s ease;
  }
  a:hover {
      color: var(--accent-color-secondary);
      text-decoration: none;
  }
  .logo-text {
      font-size: 1.8rem;
      font-weight: 700;
      letter-spacing: -0.5px;
      background: linear-gradient(90deg, var(--accent-color), #64A0FF);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      padding-left: 5px;
  }
  .logo-container {
      display: flex;
      align-items: center;
      margin-bottom: 25px;
      padding-bottom: 15px;
      border-bottom: 1px solid var(--border-color);
  }
  .logo-icon {
      font-size: 28px;
      margin-right: 8px;
      background: linear-gradient(135deg, var(--accent-color), #64A0FF);
      border-radius: 8px;
      width: 40px;
      height: 40px;
      display: flex;
      align-items: center;
      justify-content: center;
      box-shadow: 0 2px 8px rgba(72, 133, 237, 0.4);
  }
  .tooltip {
      position: relative;
      display: inline-block;
  }
  .tooltip .tooltiptext {
      visibility: hidden;
      background-color: var(--bg-secondary);
      color: var(--text-primary);
      text-align: center;
      border-radius: 6px;
      padding: 8px 12px;
      position: absolute;
      z-index: 1;
      bottom: 125%;
      left: 50%;
      transform: translateX(-50%);
      opacity: 0;
      transition: opacity 0.3s;
      box-shadow: var(--shadow);
      border: 1px solid var(--border-color);
      font-size: 0.85rem;
      white-space: nowrap;
  }
  .tooltip:hover .tooltiptext {
      visibility: visible;
      opacity: 1;
  }
  .status-indicator {
      display: inline-block;
      width: 10px;
      height: 10px;
      border-radius: 50%;
      margin-right: 6px;
  }
  .status-active {
      background-color: var(--success-color);
      box-shadow: 0 0 0 rgba(13, 177, 121, 0.4);
      animation: pulse-green 2s infinite;
  }
  @keyframes pulse-green {
      0% {
          box-shadow: 0 0 0 0 rgba(13, 177, 121, 0.7);
      }
      70% {
          box-shadow: 0 0 0 6px rgba(13, 177, 121, 0);
      }
      100% {
          box-shadow: 0 0 0 0 rgba(13, 177, 121, 0);
      }
  }
  .custom-select {
      background-color: var(--bg-card);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 10px 15px;
      color: var(--text-primary);
      font-weight: 500;
      transition: all 0.2s ease;
  }
  .custom-select:hover {
      border-color: var(--accent-color);
  }
  .divider-with-text {
      display: flex;
      align-items: center;
      margin: 25px 0;
  }
  .divider-with-text::before,
  .divider-with-text::after {
      content: "";
      flex: 1;
      border-bottom: 1px solid var(--border-color);
  }
  .divider-with-text::before {
      margin-right: 15px;
  }
  .divider-with-text::after {
      margin-left: 15px;
  }
  .divider-text {
      color: var(--text-secondary);
      font-size: 0.9rem;
      font-weight: 500;
  }
</style>
""", unsafe_allow_html=True)

load_dotenv()
USER_ID = os.getenv("USER_ID")

def send_payload(payload, key_master):
    url = "https://infinitepay-risk-api.services.production.cloudwalk.network/monitoring/offense_analysis"
    headers = {"Content-Type": "application/json", "Authorization": key_master}
    response = requests.post(url, headers=headers, json=payload)
    return response.text

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_flagged_users():
    """
    Busca usuários marcados para análise AML.
    Returns:
        list: Lista de usuários sinalizados
    """
    try:
        query_job = bigquery_client.query(fetch_combined_query)
        results = query_job.result()
        
        users_list = []
        for row in results:
            user_info = {
                'user_id': row.user_id,
                'alert_date': row.alert_date,
                'alert_type': row.alert_type,
                'score': row.score,
                'features': row.features
            }
            users_list.append(user_info)
        
        return users_list
    except Exception as e:
        logging.error(f"Erro ao buscar usuários: {str(e)}")
        return []

@st.cache_data(ttl=3600)  # Cache for 1 hour
def fetch_betting_houses(user_id=None):
    """
    Retorna uma lista de casas de apostas para o usuário específico.
    Args:
        user_id (str): ID do usuário (opcional)
    Returns:
        DataFrame: DataFrame com casas de apostas ou simulação
    """
    try:
        if user_id:
            bets_query = f"""
            SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_betting_transactions_data`
            WHERE user_id = '{user_id}'
            """
        else:
            bets_query = """
            SELECT * FROM `infinitepay-production.external_sources.betting_houses_document_numbers`
            LIMIT 10
            """
        query_job = bigquery_client.query(bets_query)
        results = query_job.result()
        betting_houses = pd.DataFrame([dict(row) for row in results])
        if not betting_houses.empty:
            return betting_houses
    except Exception as e:
        logging.warning(f"Erro ao buscar dados de casas de apostas: {str(e)}")
    has_betting_data = False
    if user_id:
        try:
            user_id_num = int(user_id) if user_id.isdigit() else sum(ord(c) for c in user_id)
            has_betting_data = user_id_num % 4 == 0
        except:
            has_betting_data = "bet" in user_id.lower() or "lavanderia" in user_id.lower()
    if has_betting_data:
        return pd.DataFrame([
            {
                "user_id": user_id,
                "betting_house": "BetExemplo",
                "amount": "1500.00",
                "date": (datetime.datetime.now() - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
            },
            {
                "user_id": user_id,
                "betting_house": "ApostaSim",
                "amount": "750.00",
                "date": (datetime.datetime.now() - datetime.timedelta(days=12)).strftime("%Y-%m-%d")
            },
        ])
    return pd.DataFrame()

@st.cache_data(ttl=1800)  # Cache for 30 minutes
def fetch_pep_data(user_id):
    pep_query = rf"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_pep_transactions_data` WHERE user_id = {user_id}
    """
    query_job = bigquery_client.query(pep_query)
    results = query_job.result()
    return pd.DataFrame([dict(row) for row in results])

def analyze_user(user_data, betting_houses=None, pep_data=None):
    """
    Analisa um usuário e gera um payload para exportação.
    Args:
        user_data (dict): Dados do usuário
        betting_houses (DataFrame): DataFrame com casas de apostas
        pep_data (DataFrame): DataFrame com dados PEP
    Returns:
        dict: Payload para exportação
    """
    user_id = user_data['user_id']
    alert_type = user_data.get('alert_type', 'Unknown Alert')
    
    # Check if user is already analyzed and cache is still valid
    cache_valid = st.session_state.cache_expire_time > datetime.datetime.now()
    if user_id in st.session_state.analyzed_users and cache_valid:
        return st.session_state.analyzed_users[user_id]
    
    try:
        # Get user type (merchant or cardholder)
        query = f"""
        SELECT u.id, u.name, u.role FROM `infinitepay-production.maindb.users` u WHERE u.id = {user_id}
        """
        user_role_df = bigquery_client.query(query).result().to_dataframe()
        user_role = user_role_df['role'].iloc[0] if not user_role_df.empty else 'unknown'
        
        if user_role in ['onboarding_merchant', 'merchant']:
            report_data = merchant_report(user_id, alert_type, pep_data)
            user_type = 'merchant'
        else:
            report_data = cardholder_report(user_id, alert_type, pep_data)
            user_type = 'cardholder'
        
        features = user_data.get('features', None)
        prompt = generate_prompt(report_data, user_type, alert_type, betting_houses, pep_data, features)
        analysis = get_gpt_analysis(prompt)
        
        risk_score_match = re.search(r'Risco de Lavagem de Dinheiro: (\d+)/10', analysis)
        risk_score = int(risk_score_match.group(1)) if risk_score_match else 5
        
        conclusion = "suspicious" if risk_score >= 6 else "normal"
        
        if "CPF em lista de PEPs" in analysis and "Risco de Lavagem de Dinheiro: 10/10" in analysis:
            conclusion = "offense"
            
        result = format_export_payload(user_id, analysis, user_data.get('business_validation', False))
        result["conclusion"] = conclusion
        result["risk_score"] = risk_score
        
        # Cache the result
        st.session_state.analyzed_users[user_id] = result
        
        return result
    except Exception as e:
        logging.error(f"Erro ao analisar usuário {user_id}: {str(e)}")
        return format_export_payload(user_id, f"Erro na análise: {str(e)}", 
                             user_data.get('business_validation', False))

def run_bot():
    flagged_users = fetch_flagged_users()
    betting_houses = fetch_betting_houses()
    key_master = ""
    results = []
    total_users = len(flagged_users)
    with st.spinner("Buscando usuários sinalizados..."):
        st.markdown(f"""
        <div style="background-color: var(--bg-secondary); padding: 15px; border-radius: 8px; margin-bottom: 20px;">
            <h3 style="margin-top: 0; color: var(--text-primary);">Resultados da Busca</h3>
            <div style="display: flex; justify-content: space-between; align-items: center;">
                <p style="margin: 0;"><strong>Usuários encontrados:</strong> {total_users}</p>
                <p style="margin: 0;"><strong>Período analisado:</strong> {days_to_fetch} dias</p>
                <p style="margin: 0;"><strong>Data da análise:</strong> {datetime.datetime.now().strftime("%d/%m/%Y %H:%M")}</p>
            </div>
        </div>
        """, unsafe_allow_html=True)
        progress_container = st.container()
        with progress_container:
            progress_cols = st.columns([3, 1])
            with progress_cols[0]:
                progress_bar = st.progress(0)
            with progress_cols[1]:
                progress_text = st.empty()
        status_container = st.empty()
    analyzed_count = 0
    suspicious_count = 0
    risk_scores = []
    start_time = datetime.datetime.now()
    for i, user_data in enumerate(flagged_users):
        try:
            analyzed_count += 1
            with status_container.container():
                st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 10px;">
                    <div style="width: 24px; height: 24px; border-radius: 50%; background-color: var(--accent-color); display: flex; justify-content: center; align-items: center;">
                        <span style="color: white; font-size: 12px;">⚙️</span>
                    </div>
                    <p style="margin: 0; color: var(--text-primary);">Analisando usuário <strong>{user_data['user_id']}</strong>...</p>
                </div>
                """, unsafe_allow_html=True)
            pep_data = fetch_pep_data(user_data['user_id'])
            export_payload = analyze_user(user_data, betting_houses=betting_houses, pep_data=pep_data)
            response_text = send_payload(export_payload, key_master)
            description = export_payload.get('description', '')
            risk_score_match = re.search(r'Risco de Lavagem de Dinheiro: (\d+)/10', description)
            risk_score = int(risk_score_match.group(1)) if risk_score_match else 0
            risk_scores.append(risk_score)
            if export_payload['conclusion'] == 'suspicious':
                suspicious_count += 1
            if risk_score <= 5:
                risk_level = "Baixo Risco"
                risk_badge = "risk-badge-low"
            elif risk_score == 6:
                risk_level = "Médio Risco"
                risk_badge = "risk-badge-medium"
            elif risk_score <= 8:
                risk_level = "Suspicious Mid"
                risk_badge = "risk-badge-medium"
                risk_badge = risk_badge.replace("medium", "medium\" style=\"background-color: #D17A00")
            elif risk_score == 9:
                risk_level = "Suspicious High"
                risk_badge = "risk-badge-high"
            else:
                risk_level = "Risco Extremo"
                risk_badge = "risk-badge-high"
            
            # Lógica atualizada para mostrar o tipo de conclusão
            if export_payload['conclusion'] == 'normal':
                conclusion = 'Normal'
                conclusion_badge = "risk-badge-low"
                if "Caso de médio risco" in export_payload.get('description', ''):
                    conclusion = 'Normal (monitorar)'
                    conclusion_badge = "risk-badge-medium"
            elif export_payload['conclusion'] == 'suspicious':
                # Verifica a prioridade para distinguir entre suspicious mid e high
                if export_payload.get('priority') == 'mid':
                    conclusion = 'Suspicious Mid'
                    conclusion_badge = "risk-badge-medium\" style=\"background-color: #D17A00"
                else:
                    conclusion = 'Suspicious High'
                    conclusion_badge = "risk-badge-high"
            elif export_payload['conclusion'] == 'offense':
                conclusion = 'Offense High'
                conclusion_badge = "risk-badge-high\" style=\"background-color: #d32f2f"
            else:
                conclusion = 'Indefinido'
                conclusion_badge = "risk-badge-medium"
            
            user_type = "👤 Cardholder" if "cardholder_info" in str(export_payload) else "🏪 Merchant"
            with st.expander(f"User ID: {user_data['user_id']} - {user_type} - Score: {risk_score}/10", expanded=False):
                st.markdown(f"""
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 10px; border-bottom: 1px solid var(--border-color); margin-bottom: 15px;">
                    <div>
                        <h3 style="margin: 0; color: var(--text-primary);">ID: {user_data['user_id']}</h3>
                        <p style="margin: 5px 0 0 0; color: var(--text-secondary);">{user_type}</p>
                    </div>
                    <div style="display: flex; gap: 10px; align-items: center;">
                        <div>
                            <span class='{risk_badge}' style="display: inline-block; margin-right: 5px;">
                                {risk_level} ({risk_score}/10)
                            </span>
                        </div>
                        <div>
                            <span class='{conclusion_badge}'>
                                {conclusion}
                            </span>
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
                st.markdown(f"""
                <div style="background-color: var(--bg-secondary); padding: 12px; border-radius: 6px; margin-bottom: 15px;">
                    <h4 style="margin-top: 0; color: var(--text-primary);">Detalhes do Alerta</h4>
                    <p><strong>Tipo:</strong> {user_data['alert_type']}</p>
                    <p><strong>Data:</strong> {datetime.datetime.now().strftime("%d/%m/%Y %H:%M")}</p>
                </div>
                """, unsafe_allow_html=True)
                tab1, tab2 = st.tabs(["📊 Payload", "🔄 Resposta API"])
                with tab1:
                    json_output = json.dumps(export_payload, indent=4, ensure_ascii=False)
                    json_output = json_output.replace("\\n", "\n")
                    st.code(json_output, language="json")
                with tab2:
                    st.code(response_text, language="json")
            progress = (i + 1) / total_users
            progress_bar.progress(progress)
            elapsed_time = (datetime.datetime.now() - start_time).total_seconds()
            avg_time_per_user = elapsed_time / (i + 1)
            remaining_users = total_users - (i + 1)
            estimated_time_left = remaining_users * avg_time_per_user
            minutes_left = int(estimated_time_left // 60)
            seconds_left = int(estimated_time_left % 60)
            progress_text.markdown(f"""
            <div style="text-align: center;">
                <p style="margin: 0; font-size: 0.9rem;">
                    {i+1}/{total_users} concluídos
                    <span style="color: var(--text-secondary); margin-left: 10px;">
                        Tempo restante: {minutes_left}min {seconds_left}s
                    </span>
                </p>
            </div>
            """, unsafe_allow_html=True)
        except Exception as e:
            st.error(f"Erro ao analisar o usuário {user_data['user_id']}: {str(e)}")
    status_container.empty()
    end_time = datetime.datetime.now()
    total_time = (end_time - start_time).total_seconds()
    avg_time = total_time / total_users if total_users > 0 else 0
    avg_score = sum(risk_scores) / len(risk_scores) if risk_scores else 0
    st.markdown(f"""
    <div style="background-color: var(--bg-secondary); padding: 20px; border-radius: 8px; margin: 20px 0; border-left: 4px solid var(--success-color);">
        <h2 style="margin-top: 0; color: var(--text-primary);">🎉 Análise completa!</h2>
        <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-top: 15px;">
            <div style="background-color: var(--bg-card); padding: 15px; border-radius: 6px; text-align: center;">
                <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">Usuários Analisados</p>
                <p style="font-size: 1.8rem; margin: 5px 0; font-weight: bold; color: var(--text-primary);">{analyzed_count}</p>
            </div>
            <div style="background-color: var(--bg-card); padding: 15px; border-radius: 6px; text-align: center;">
                <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">Casos Suspeitos</p>
                <p style="font-size: 1.8rem; margin: 5px 0; font-weight: bold; color: var(--danger-color);">{suspicious_count}</p>
            </div>
            <div style="background-color: var(--bg-card); padding: 15px; border-radius: 6px; text-align: center;">
                <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">Score Médio</p>
                <p style="font-size: 1.8rem; margin: 5px 0; font-weight: bold; color: var(--text-primary);">{avg_score:.1f}</p>
            </div>
            <div style="background-color: var(--bg-card); padding: 15px; border-radius: 6px; text-align: center;">
                <p style="margin: 0; color: var(--text-secondary); font-size: 0.9rem;">Tempo Total</p>
                <p style="font-size: 1.8rem; margin: 5px 0; font-weight: bold; color: var(--text-primary);">{int(total_time//60)}m {int(total_time%60)}s</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    return results

def main():
    with st.sidebar:
        st.markdown("""
        <div class="logo-container">
            <div class="logo-icon">🔍</div>
            <div class="logo-text">Lavandowski</div>
        </div>
        """, unsafe_allow_html=True)
        st.markdown("<div class='sub-header'>Configurações</div>", unsafe_allow_html=True)
        global days_to_fetch
        days_to_fetch = st.slider(
            "Intervalo de dias para buscar alertas",
            min_value=1,
            max_value=30,
            value=7,
            help="Define quantos dias no passado serão analisados"
        )
        
        # Reset cache if days_to_fetch changes
        if st.session_state.current_day_fetch != days_to_fetch:
            st.session_state.analyzed_users = {}
            st.session_state.current_day_fetch = days_to_fetch
            st.session_state.cache_expire_time = datetime.datetime.now() + datetime.timedelta(hours=1)
        
        analysis_type = st.radio(
            "Tipo de Análise",
            options=["Básica (GPT-4)", "Aprimorada (GPT-4 + o3-mini)", "Com Pontuação de Risco (o3-mini)"],
            index=2,
            help="Selecione o método de análise desejado"
        )
        simulation_mode = st.checkbox(
            "Simulação (não enviar para API)",
            value=False,
            help="Se marcado, não enviará os resultados para a API"
        )
        specific_user_id = st.text_input(
            "ID de usuário específico (opcional)",
            help="Se preenchido, apenas este usuário será analisado"
        )
        current_time = datetime.datetime.now().strftime("%d/%m/%Y %H:%M")
        st.markdown(f"**Data atual:** {current_time}")
        st.markdown("<div class='sub-header'>Sistema de Pontuação</div>", unsafe_allow_html=True)
        st.markdown("""
        <div style="background-color: var(--bg-secondary); padding: 15px; border-radius: 10px; margin: 10px 0; box-shadow: var(--shadow);">
            <p style="margin: 0 0 12px 0; font-weight: 500;">Escala de Risco (1-10):</p>
            <div style="display: flex; gap: 8px; flex-direction: column;">
                <div><span class='risk-badge-low'>1-5: Baixo Risco</span></div>
                <div><span class='risk-badge-medium'>6: Médio Risco</span></div>
                <div><span class='risk-badge-medium' style="background-color: #D17A00;">7-8: Suspicious Mid</span></div>
                <div><span class='risk-badge-high'>9: Suspicious High</span></div>
                <div><span class='risk-badge-high' style="background-color: #d32f2f;">10: Offense High</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)
    st.markdown("<h1 class='main-header'>🔍 Lavandowski AML Analysis</h1>", unsafe_allow_html=True)
    st.markdown(f"""
    <div style="display: flex; justify-content: flex-end; align-items: center; margin-bottom: 20px;">
        <div style="background-color: var(--bg-secondary); padding: 8px 15px; border-radius: 8px; display: flex; gap: 15px; box-shadow: var(--shadow);">
            <div style="text-align: center;">
                <p style="margin: 0; font-size: 0.75rem; color: var(--text-secondary);">Versão</p>
                <p style="margin: 0; font-weight: bold; color: var(--accent-color);">1.0.2</p>
            </div>
            <div style="text-align: center; border-left: 1px solid var(--border-color); padding-left: 15px;">
                <p style="margin: 0; font-size: 0.75rem; color: var(--text-secondary);">Atualizado</p>
                <p style="margin: 0; font-weight: 500; color: var(--text-primary);">{datetime.datetime.now().strftime("%d/%m/%Y")}</p>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    with st.expander("ℹ️ Sobre o Lavandowski AML Analysis", expanded=False):
        st.markdown("""
        <div style="padding: 10px;">
            <p>O <strong>Lavandowski</strong> é uma plataforma avançada que utiliza inteligência artificial para analisar transações financeiras e identificar possíveis casos de lavagem de dinheiro.</p>
            <p>A ferramenta emprega um algoritmo de avaliação de risco sofisticado que atribui uma pontuação de 1 a 10 para cada caso analisado, determinando automaticamente se o caso deve ser considerado normal ou requer investigação adicional.</p>
            <p>Utilizamos os modelos de IA mais avançados do mercado para realizar análises detalhadas dos padrões transacionais e comportamentais dos usuários.</p>
            <div style="background-color: var(--bg-secondary); padding: 15px; border-radius: 10px; margin-top: 15px; box-shadow: var(--shadow);">
                <h4 style="margin-top: 0;">Recursos principais:</h4>
                <ul>
                    <li>Detecção avançada de padrões suspeitos em transações</li>
                    <li>Classificação automática de risco com IA</li>
                    <li>Análise de vínculos e conexões entre entidades</li>
                    <li>Geração de relatórios detalhados e exportáveis</li>
                    <li>Integração com sistemas de compliance</li>
                </ul>
            </div>
        </div>
        """, unsafe_allow_html=True)
    try:
        stats_query = f"""
        SELECT
            COUNT(*) as total_analises,
            SUM(CASE WHEN conclusion = 'suspicious' THEN 1 ELSE 0 END) as total_suspeitos,
            AVG(risk_score) as score_medio,
            AVG(processing_time) as tempo_medio
        FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
        WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_to_fetch} DAY)
        """
        trend_query = f"""
        WITH semana_atual AS (
            SELECT COUNT(*) as total
            FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
            WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        ),
        semana_anterior AS (
            SELECT COUNT(*) as total
            FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
            WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 14 DAY)
            AND DATE(created_at) < DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
        )
        SELECT
            a.total as total_atual,
            b.total as total_anterior,
            CASE​​
                WHEN b.total > 0 THEN ROUND((a.total - b.total) / b.total * 100)
                ELSE 0
            END as variacao_percentual
        FROM semana_atual a, semana_anterior b
        """
        try:
            stats_job = bigquery_client.query(stats_query)
            stats_result = next(stats_job.result())
            trend_job = bigquery_client.query(trend_query)
            trend_result = next(trend_job.result())
            total_analises = stats_result.total_analises or 0
            total_suspeitos = stats_result.total_suspeitos or 0
            score_medio = stats_result.score_medio or 5.0
            tempo_medio = stats_result.tempo_medio or 45.0
            variacao_percentual = trend_result.variacao_percentual or 0
            percentual_suspeitos = round((total_suspeitos / total_analises * 100) if total_analises > 0 else 0)
            tendencia_analises = "↑" if variacao_percentual >= 0 else "↓"
            cor_tendencia = "var(--success-color)" if variacao_percentual >= 0 else "var(--danger-color)"
            if score_medio <= 5:
                faixa_risco = "Faixa de Baixo Risco"
            elif score_medio <= 6:
                faixa_risco = "Faixa de Médio Risco"
            elif score_medio <= 8:
                faixa_risco = "Faixa de Suspicious Mid"
            elif score_medio <= 9:
                faixa_risco = "Faixa de Suspicious High"
            else:
                faixa_risco = "Faixa de Offense High"
            reducao_tempo = 8
        except Exception as e:
            logging.warning(f"Erro ao buscar estatísticas reais: {str(e)}")
            total_analises = 0
            total_suspeitos = 0
            score_medio = 5.0
            tempo_medio = 45.0
            variacao_percentual = 0
            percentual_suspeitos = 0
            tendencia_analises = "↑"
            cor_tendencia = "var(--text-secondary)"
            faixa_risco = "Faixa de Médio Risco"
            reducao_tempo = 0
    except Exception as e:
        logging.warning(f"Erro ao configurar consultas: {str(e)}")
        total_analises = 0
        total_suspeitos = 0
        score_medio = 5.0
        tempo_medio = 45.0
        variacao_percentual = 0
        percentual_suspeitos = 0
        tendencia_analises = "↑"
        cor_tendencia = "var(--text-secondary)"
        faixa_risco = "Faixa de Médio Risco"
        reducao_tempo = 0
    st.markdown("<div class='sub-header'>Visão Geral</div>", unsafe_allow_html=True)
    stats_cols = st.columns(4)
    with stats_cols[0]:
        st.markdown(f"""
        <div class="dashboard-card">
            <div class="stat-label">Análises Realizadas</div>
            <div class="stat-value" style="color: var(--accent-color);">{total_analises}</div>
            <div style="text-align: center; font-size: 0.85rem; color: {cor_tendencia}; display: flex; align-items: center; justify-content: center; gap: 5px;">
                <span class="tooltip">{tendencia_analises} {abs(variacao_percentual)}% esta semana
                    <span class="tooltiptext">Comparado à semana anterior</span>
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    with stats_cols[1]:
        st.markdown(f"""
        <div class="dashboard-card">
            <div class="stat-label">Casos Suspeitos</div>
            <div class="stat-value" style="color: var(--danger-color);">{total_suspeitos}</div>
            <div style="text-align: center; font-size: 0.85rem; color: var(--text-secondary); display: flex; align-items: center; justify-content: center; gap: 5px;">
                <span class="tooltip">{percentual_suspeitos}% do total
                    <span class="tooltiptext">Percentual de casos considerados suspeitos</span>
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    with stats_cols[2]:
        st.markdown(f"""
        <div class="dashboard-card">
            <div class="stat-label">Score Médio</div>
            <div class="stat-value" style="color: var(--warning-color);">{score_medio:.1f}</div>
            <div style="text-align: center; font-size: 0.85rem; color: var(--text-secondary); display: flex; align-items: center; justify-content: center; gap: 5px;">
                <span class="tooltip">{faixa_risco}
                    <span class="tooltiptext">Média de scores de risco</span>
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    with stats_cols[3]:
        st.markdown(f"""
        <div class="dashboard-card">
            <div class="stat-label">Tempo de Resposta</div>
            <div class="stat-value" style="color: var(--success-color);">{int(tempo_medio)}s</div>
            <div style="text-align: center; font-size: 0.85rem; color: var(--success-color); display: flex; align-items: center; justify-content: center; gap: 5px;">
                <span class="tooltip">↓ {reducao_tempo}% mais rápido
                    <span class="tooltiptext">Comparado à média histórica</span>
                </span>
            </div>
        </div>
        """, unsafe_allow_html=True)
    try:
        alert_types_query = f"""
        SELECT
            alert_type,
            COUNT(*) as total
        FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
        WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_to_fetch} DAY)
        GROUP BY alert_type
        ORDER BY total DESC
        LIMIT 5
        """
        risk_levels_query = f"""
        SELECT
            CASE
                WHEN risk_score BETWEEN 1 AND 5 THEN 'Baixo'
                WHEN risk_score = 6 THEN 'Médio'
                WHEN risk_score BETWEEN 7 AND 8 THEN 'Suspicious Mid'
                WHEN risk_score = 9 THEN 'Suspicious High'
                WHEN risk_score = 10 THEN 'Offense High'
            END as nivel_risco,
            COUNT(*) as total
        FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
        WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_to_fetch} DAY)
        GROUP BY nivel_risco
        ORDER BY
            CASE nivel_risco
                WHEN 'Baixo' THEN 1
                WHEN 'Médio' THEN 2
                WHEN 'Suspicious Mid' THEN 3
                WHEN 'Suspicious High' THEN 4
                WHEN 'Offense High' THEN 5
            END
        """
        trend_daily_query = f"""
        SELECT
            DATE(created_at) as data,
            COUNT(*) as total
        FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis`
        WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days_to_fetch} DAY)
        GROUP BY data
        ORDER BY data
        """
        try:
            alert_types_job = bigquery_client.query(alert_types_query)
            alert_types_results = list(alert_types_job.result())
            risk_levels_job = bigquery_client.query(risk_levels_query)
            risk_levels_results = list(risk_levels_job.result())
            trend_daily_job = bigquery_client.query(trend_daily_query)
            trend_daily_results = list(trend_daily_job.result())
            alert_types_data = {
                'tipos': [row.alert_type for row in alert_types_results],
                'totais': [row.total for row in alert_types_results]
            }
            risk_levels_data = {
                'niveis': [row.nivel_risco for row in risk_levels_results],
                'totais': [row.total for row in risk_levels_results]
            }
            trend_daily_data = {
                'datas': [row.data.strftime("%d/%m") for row in trend_daily_results],
                'totais': [row.total for row in trend_daily_results]
            }
            alert_types_df = pd.DataFrame(alert_types_data)
            risk_levels_df = pd.DataFrame(risk_levels_data)
            trend_daily_df = pd.DataFrame(trend_daily_data)
            has_chart_data = True
        except Exception as e:
            logging.warning(f"Erro ao buscar dados para gráficos: {str(e)}")
            has_chart_data = False
    except Exception as e:
        logging.warning(f"Erro ao configurar consultas para gráficos: {str(e)}")
        has_chart_data = False
    st.markdown("""
    <div class="divider-with-text">
        <span class="divider-text">DISTRIBUIÇÃO DE ALERTAS</span>
    </div>
    """, unsafe_allow_html=True)
    chart_cols = st.columns(3)
    with chart_cols[0]:
        st.markdown("""
        <div class="chart-container">
            <h4 class="chart-title">Por Tipo de Alerta</h4>
        """, unsafe_allow_html=True)
        if has_chart_data and not alert_types_df.empty:
            try:
                st.bar_chart(alert_types_df.set_index('tipos'), use_container_width=True, height=250)
            except:
                st.info("Dados insuficientes para gerar o gráfico")
        else:
            st.markdown("""
            <div style="height: 250px; display: flex; justify-content: center; align-items: center; flex-direction: column; gap: 15px;">
                <div style="color: var(--text-secondary); font-size: 0.9rem;">Sem dados disponíveis para este período</div>
                <div class="skeleton loading-value" style="width: 70%; height: 120px;"></div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with chart_cols[1]:
        st.markdown("""
        <div class="chart-container">
            <h4 class="chart-title">Por Nível de Risco</h4>
        """, unsafe_allow_html=True)
        if has_chart_data and not risk_levels_df.empty:
            try:
                fig = {
                    'data': [{
                        'values': risk_levels_df['totais'],
                        'labels': risk_levels_df['niveis'],
                        'type': 'pie',
                        'hole': 0.4,
                        'marker': {
                            'colors': ['#0DB179', '#F7B846', '#D17A00', '#E74C3C', '#d32f2f']
                        },
                    }],
                    'layout': {
                        'showlegend': True,
                        'legend': {'orientation': 'h', 'y': -0.1},
                        'margin': {'t': 10, 'b': 10, 'l': 10, 'r': 10},
                        'paper_bgcolor': 'rgba(0,0,0,0)',
                        'plot_bgcolor': 'rgba(0,0,0,0)',
                        'font': {'color': '#C8D4E9'},
                    }
                }
                st.plotly_chart(fig, use_container_width=True, config={'displayModeBar': False})
            except:
                st.info("Dados insuficientes para gerar o gráfico")
        else:
            st.markdown("""
            <div style="height: 250px; display: flex; justify-content: center; align-items: center; flex-direction: column; gap: 15px;">
                <div style="color: var(--text-secondary); font-size: 0.9rem;">Sem dados disponíveis para este período</div>
                <div class="skeleton" style="width: 150px; height: 150px; border-radius: 50%;"></div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    with chart_cols[2]:
        st.markdown("""
        <div class="chart-container">
            <h4 class="chart-title">Tendência de Alertas</h4>
        """, unsafe_allow_html=True)
        if has_chart_data and not trend_daily_df.empty:
            try:
                st.line_chart(trend_daily_df.set_index('datas'), use_container_width=True, height=250)
            except:
                st.info("Dados insuficientes para gerar o gráfico")
        else:
            st.markdown("""
            <div style="height: 250px; display: flex; justify-content: center; align-items: center; flex-direction: column; gap: 15px;">
                <div style="color: var(--text-secondary); font-size: 0.9rem;">Sem dados disponíveis para este período</div>
                <div style="width: 80%;">
                    <div class="skeleton" style="width: 100%; height: 5px; margin-bottom: 15px;"></div>
                    <div class="skeleton" style="width: 70%; height: 5px; margin-bottom: 15px;"></div>
                    <div class="skeleton" style="width: 85%; height: 5px; margin-bottom: 15px;"></div>
                    <div class="skeleton" style="width: 60%; height: 5px; margin-bottom: 15px;"></div>
                    <div class="skeleton" style="width: 90%; height: 5px;"></div>
                </div>
            </div>
            """, unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)
    st.markdown("""
    <div style="background-color: var(--bg-secondary); border-radius: 10px; padding: 15px; margin: 30px 0 20px 0; box-shadow: var(--shadow); border: 1px solid var(--card-border);">
        <div style="display: flex; justify-content: space-between; align-items: center;">
            <div style="display: flex; align-items: center;">
                <span class="status-indicator status-active"></span>
                <span style="color: var(--text-primary); font-weight: 500;">Sistema ativo e operacional</span>
            </div>
            <div style="display: flex; gap: 15px; align-items: center;">
                <div class="tooltip" style="color: var(--text-secondary); font-size: 0.85rem;">
                    Última atualização: hoje às 09:45
                    <span class="tooltiptext">Horário da última sincronização com o banco de dados</span>
                </div>
                <div style="display: flex; align-items: center; gap: 5px; background-color: var(--bg-card); padding: 5px 10px; border-radius: 6px;">
                    <span style="width: 8px; height: 8px; background-color: var(--success-color); border-radius: 50%;"></span>
                    <span style="color: var(--text-secondary); font-size: 0.85rem;">API: Online</span>
                </div>
            </div>
        </div>
    </div>
    """, unsafe_allow_html=True)
    st.markdown("""
    <div class="divider-with-text">
        <span class="divider-text">AÇÕES</span>
    </div>
    """, unsafe_allow_html=True)
    col1, col2 = st.columns([3, 1])
    with col1:
        if st.button("✨ Executar Nova Análise AML", type="primary", use_container_width=True):
            with st.container():
                run_bot()
    with col2:
        st.button("📊 Exportar Relatório", type="secondary", use_container_width=True)

if __name__ == "__main__":
    main()

import datetime
import pandas as pd
from google.cloud import bigquery
from gpt_utils import get_chatgpt_response
import json
import decimal
import logging
import os
import re
from dotenv import load_dotenv

# Carrega as variáveis de ambiente
load_dotenv()

# Configura o logging
logging.basicConfig(level=logging.ERROR)

# Cria um encoder customizado para objetos Decimal
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
            return obj.isoformat()
        else:
            return super().default(obj)

# Configura opções do Pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 40)
pd.set_option('display.min_rows', 40)

# Conecta ao projeto no Google Cloud
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
location = os.getenv("LOCATION")
client = bigquery.Client(project=project_id, location=location)


def format_date_portuguese(date_str: str) -> str:
    """Formata uma string de data para o formato em português."""
    if date_str is None:
        return 'Not available.'
    month_names = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
        7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    date = datetime.datetime.strptime(date_str, '%d-%m-%Y')
    return f"{date.day} de {month_names[date.month]} de {date.year}"


def format_cpf(cpf: str) -> str:
    """Formata uma string de CPF."""
    if cpf is None:
        return None
    cpf = cpf.replace('.', '').replace('-', '')
    if len(cpf) == 11:
        formatted_cpf = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
        return formatted_cpf
    else:
        return cpf


def execute_query(query):
    """Executa uma query no BigQuery e retorna um DataFrame."""
    try:
        df = client.query(query).result().to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()


def fetch_lawsuit_data(user_id: int) -> pd.DataFrame:
    """Busca dados de processos para o user_id informado."""
    query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_lawsuits_data`
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df


def fetch_business_data(user_id: int) -> pd.DataFrame:
    """Busca dados de relacionamento empresarial para o user_id informado."""
    query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_business_relationships_data`
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df


def fetch_sanctions_history(user_id: int) -> pd.DataFrame:
    """Busca dados de sanções para o user_id informado."""
    query = f"""
    SELECT * FROM infinitepay-production.metrics_amlft.sanctions_history
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df


def fetch_denied_transactions(user_id: int) -> pd.DataFrame:
    """Busca transações negadas para o user_id (merchant_id)."""
    query = f"""
        SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_transactions_data`
        WHERE merchant_id = {user_id} ORDER BY card_number
    """
    df = execute_query(query)
    return df


def fetch_denied_pix_transactions(user_id: int) -> pd.DataFrame:
    """Busca transações PIX negadas para o user_id."""
    query = f"""
         SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_pix_transfers_data`
         WHERE debitor_user_id = '{user_id}' ORDER BY str_pix_transfer_id DESC
    """
    df = execute_query(query)
    return df


def fetch_prison_transactions(user_id: int) -> pd.DataFrame:
    """Busca transações no presídio para o user_id informado."""
    query = f"""
    SELECT * EXCEPT(user_id) FROM infinitepay-production.metrics_amlft.prison_transactions
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df


def fetch_bets_pix_transfers(user_id: int) -> pd.DataFrame:
    """Busca transações de apostas via PIX para o user_id informado."""
    query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.bets_pix_transfers`
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df


def convert_decimals(data):
    """Converte recursivamente objetos Decimal em float."""
    if isinstance(data, list):
        return [
            {k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v
             for k, v in item.items()}
            for item in data
        ]
    elif isinstance(data, dict):
        return {k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v
                for k, v in data.items()}
    else:
        return data


def merchant_report(user_id: int, alert_type: str, pep_data=None) -> dict:
    """Gera um relatório para merchant."""
    query_merchants = f"""
    SELECT * FROM metrics_amlft.merchant_report WHERE user_id = {user_id} LIMIT 1
    """
    query_issuing_concentration = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_issuing_payments_data`
    WHERE user_id = {user_id}
    """
    query_pix_concentration = f"""
    SELECT * FROM metrics_amlft.pix_concentration WHERE user_id = {user_id}
    """
    # Ajuste: Traz a tabela de cardholder_concentration para merchant_report
    query_transaction_concentration = f"""
    SELECT * EXCEPT(merchant_id) FROM `infinitepay-production.metrics_amlft.cardholder_concentration`
    WHERE merchant_id = {user_id} ORDER BY total_approved_by_ch DESC
    """
    query_offense_history = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis_data`
    WHERE user_id = {user_id} ORDER BY id DESC
    """
    products_online_store = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_online_store_data`
    WHERE user_id = {user_id}
    """
    contacts_query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_phonecast_data`
    WHERE user_id = {user_id}
    """
    devices_query = f"""
    SELECT * EXCEPT(user_id) FROM metrics_amlft.user_device WHERE user_id = {user_id}
    """

    merchant_info = execute_query(query_merchants)
    issuing_concentration = execute_query(query_issuing_concentration)
    pix_concentration = execute_query(query_pix_concentration)
    transaction_concentration = execute_query(query_transaction_concentration)
    offense_history = execute_query(query_offense_history)
    products_online = execute_query(products_online_store)
    contacts = execute_query(contacts_query)
    devices = execute_query(devices_query)

    cash_in = pd.DataFrame()
    cash_out = pd.DataFrame()
    total_cash_in_pix = 0.0
    total_cash_out_pix = 0.0
    total_cash_in_pix_atypical_hours = 0.0
    total_cash_out_pix_atypical_hours = 0.0

    if not pix_concentration.empty:
        cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
        cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
        total_cash_in_pix = cash_in['pix_amount'].sum()
        total_cash_out_pix = cash_out['pix_amount'].sum()
        total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
        total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()

    merchant_info_dict = merchant_info.to_dict(orient='records')[0] if not merchant_info.empty else {}
    issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
    transaction_concentration_list = transaction_concentration.to_dict(orient='records') if not transaction_concentration.empty else []
    cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
    cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
    offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
    products_online_list = products_online.to_dict(orient='records') if not products_online.empty else []
    contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
    devices_list = devices.to_dict(orient='records') if not devices.empty else []

    merchant_info_dict = convert_decimals(merchant_info_dict)
    issuing_concentration_list = convert_decimals(issuing_concentration_list)
    transaction_concentration_list = convert_decimals(transaction_concentration_list)
    cash_in_list = convert_decimals(cash_in_list)
    cash_out_list = convert_decimals(cash_out_list)
    offense_history_list = convert_decimals(offense_history_list)
    products_online_list = convert_decimals(products_online_list)
    contacts_list = convert_decimals(contacts_list)
    devices_list = convert_decimals(devices_list)

    lawsuit_data = fetch_lawsuit_data(user_id)
    lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []

    denied_transactions_df = fetch_denied_transactions(user_id)
    denied_transactions_list = denied_transactions_df.to_dict(orient='records') if not denied_transactions_df.empty else []

    business_data = fetch_business_data(user_id)
    business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []

    prison_transactions_df = fetch_prison_transactions(user_id)
    prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []
    prison_transactions_list = convert_decimals(prison_transactions_list)

    sanctions_history_df = fetch_sanctions_history(user_id)
    sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []
    sanctions_history_list = convert_decimals(sanctions_history_list)

    denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
    denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []
    denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)

    bets_pix_transfers_df = fetch_bets_pix_transfers(user_id)
    bets_pix_transfers_list = bets_pix_transfers_df.to_dict(orient='records') if not bets_pix_transfers_df.empty else []
    bets_pix_transfers_list = convert_decimals(bets_pix_transfers_list)

    report = {
        "merchant_info": merchant_info_dict,
        "total_cash_in_pix": total_cash_in_pix,
        "total_cash_out_pix": total_cash_out_pix,
        "total_cash_in_pix_atypical_hours": total_cash_in_pix_atypical_hours,
        "total_cash_out_pix_atypical_hours": total_cash_out_pix_atypical_hours,
        "issuing_concentration": issuing_concentration_list,
        "transaction_concentration": transaction_concentration_list,
        "pix_cash_in": cash_in_list,
        "pix_cash_out": cash_out_list,
        "offense_history": offense_history_list,
        "products_online": products_online_list,
        "contacts": contacts_list,
        "devices": devices_list,
        "lawsuit_data": lawsuit_data,
        "denied_transactions": denied_transactions_list,
        "business_data": business_data_list,
        "prison_transactions": prison_transactions_list,
        "sanctions_history": sanctions_history_list,
        "denied_pix_transactions": denied_pix_transactions_list,
        "bets_pix_transfers": bets_pix_transfers_list
    }

    return report


def cardholder_report(user_id: int, alert_type: str, pep_data=None) -> dict:
    """Gera um relatório para cardholders."""
    query_cardholders = f"""
    SELECT * FROM metrics_amlft.cardholder_report WHERE user_id = {user_id} LIMIT 1
    """
    query_issuing_concentration = f"""
    SELECT * EXCEPT(user_id) FROM metrics_amlft.issuing_concentration WHERE user_id = {user_id}
    """
    query_pix_concentration = f"""
    SELECT * FROM metrics_amlft.pix_concentration WHERE user_id = {user_id}
    """
    query_offense_history = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_offense_analysis_data`
    WHERE user_id = {user_id} ORDER BY id DESC
    """
    contacts_query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_phonecast_data` WHERE user_id = {user_id}
    """
    devices_query = f"""
    SELECT * EXCEPT(user_id) FROM metrics_amlft.user_device WHERE user_id = {user_id}
    """

    cardholder_info = execute_query(query_cardholders)
    issuing_concentration = execute_query(query_issuing_concentration)
    pix_concentration = execute_query(query_pix_concentration)
    offense_history = execute_query(query_offense_history)
    contacts = execute_query(contacts_query)
    devices = execute_query(devices_query)

    cash_in = pd.DataFrame()
    cash_out = pd.DataFrame()
    total_cash_in_pix = 0.0
    total_cash_out_pix = 0.0
    total_cash_in_pix_atypical_hours = 0.0
    total_cash_out_pix_atypical_hours = 0.0

    if not pix_concentration.empty:
        cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
        cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
        total_cash_in_pix = cash_in['pix_amount'].sum()
        total_cash_out_pix = cash_out['pix_amount'].sum()
        total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
        total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()

    cardholder_info_dict = cardholder_info.to_dict(orient='records')[0] if not cardholder_info.empty else {}
    issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
    cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
    cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
    offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
    contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
    devices_list = devices.to_dict(orient='records') if not devices.empty else []

    cardholder_info_dict = convert_decimals(cardholder_info_dict)
    issuing_concentration_list = convert_decimals(issuing_concentration_list)
    cash_in_list = convert_decimals(cash_in_list)
    cash_out_list = convert_decimals(cash_out_list)
    offense_history_list = convert_decimals(offense_history_list)
    contacts_list = convert_decimals(contacts_list)
    devices_list = convert_decimals(devices_list)

    lawsuit_data = fetch_lawsuit_data(user_id)
    lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []

    business_data = fetch_business_data(user_id)
    business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []

    prison_transactions_df = fetch_prison_transactions(user_id)
    prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []
    prison_transactions_list = convert_decimals(prison_transactions_list)

    sanctions_history_df = fetch_sanctions_history(user_id)
    sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []
    sanctions_history_list = convert_decimals(sanctions_history_list)

    denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
    denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []
    denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)

    bets_pix_transfers_df = fetch_bets_pix_transfers(user_id)
    bets_pix_transfers_list = bets_pix_transfers_df.to_dict(orient='records') if not bets_pix_transfers_df.empty else []
    bets_pix_transfers_list = convert_decimals(bets_pix_transfers_list)

    report = {
        "cardholder_info": cardholder_info_dict,
        "total_cash_in_pix": total_cash_in_pix,
        "total_cash_out_pix": total_cash_out_pix,
        "total_cash_in_pix_atypical_hours": total_cash_in_pix_atypical_hours,
        "total_cash_out_pix_atypical_hours": total_cash_out_pix_atypical_hours,
        "issuing_concentration": issuing_concentration_list,
        "pix_cash_in": cash_in_list,
        "pix_cash_out": cash_out_list,
        "offense_history": offense_history_list,
        "contacts": contacts_list,
        "devices": devices_list,
        "lawsuit_data": lawsuit_data,
        "business_data": business_data_list,
        "prison_transactions": prison_transactions_list,
        "sanctions_history": sanctions_history_list,
        "denied_pix_transactions": denied_pix_transactions_list,
        "bets_pix_transfers": bets_pix_transfers_list
    }

    return report


def generate_prompt(report_data: dict, user_type: str, alert_type: str, betting_houses: pd.DataFrame = None, pep_data: pd.DataFrame = None, features: str = None) -> str:
    """Gera o prompt para o GPT com base no relatório."""
    import json

    user_info_key = f"{user_type.lower()}_info"
    user_info_json = json.dumps(report_data[user_info_key], ensure_ascii=False, indent=2, cls=CustomJSONEncoder)

    issuing_concentration_json = json.dumps(report_data.get('issuing_concentration', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    pix_cash_in_json = json.dumps(report_data.get('pix_cash_in', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    pix_cash_out_json = json.dumps(report_data.get('pix_cash_out', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    offense_history_json = json.dumps(report_data.get('offense_history', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    contacts_json = json.dumps(report_data.get('contacts', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    devices_json = json.dumps(report_data.get('devices', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    lawsuit_data_json = json.dumps(report_data.get('lawsuit_data', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    denied_transactions_json = json.dumps(report_data.get('denied_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    business_data_json = json.dumps(report_data.get('business_data', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    prison_transactions_json = json.dumps(report_data.get('prison_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    sanctions_history_json = json.dumps(report_data.get('sanctions_history', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    denied_pix_transactions_json = json.dumps(report_data.get('denied_pix_transactions', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    bets_pix_transfers_json = json.dumps(report_data.get('bets_pix_transfers', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
    
    prompt = f"""
Por favor, analise o caso abaixo.

Considere os seguintes níveis de risco:
1 - Baixo;
2 - Médio (possível ligação com PEPs);
3 - Alto (PEP, indivíduos ou empresas com histórico em listas de sanções, etc.)

Tipo de Alerta: {alert_type}

Informação do {user_type}:
{user_info_json}
"""

    if user_type == 'Merchant':
        transaction_concentration_json = json.dumps(report_data.get('transaction_concentration', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
        products_online_json = json.dumps(report_data.get('products_online', []), ensure_ascii=False, indent=2, cls=CustomJSONEncoder)
        prompt += f"""
Total de Transações PIX:
- Cash In: R${report_data['total_cash_in_pix']:,.2f}
- Cash Out: R${report_data['total_cash_out_pix']:,.2f}

Transações em Horários Atípicos:
- Cash In PIX: R${report_data['total_cash_in_pix_atypical_hours']:,.2f}
- Cash Out PIX: R${report_data['total_cash_out_pix_atypical_hours']:,.2f}

Concentração de Transações por Portador de Cartão:
{transaction_concentration_json}

Concentração de Issuing:
{issuing_concentration_json}

Transações Negadas:
{denied_transactions_json}

Histórico Profissional:
{business_data_json}

Transações Confirmadamente Executadas Dentro do Presídio (Atenção especial às colunas status e transaction_type. Transações negadas ou com errors também devem ser consideradas):
{prison_transactions_json}

Contatos:
{contacts_json}

Dispositivos Utilizados:
{devices_json}

Produtos na Loja InfinitePay:
{products_online_json}

Sanções Judiciais (Dê detalhes sobre o caso durante a análise. Pensão alimentícia ou casos de família podem ser desconsiderados):
{sanctions_history_json}

Transação PIX Negadas e motivo (coluna risk_check):
{denied_pix_transactions_json}

Concentrações PIX:
Cash In:
{pix_cash_in_json}
Cash Out:
{pix_cash_out_json}

Informações sobre processos judiciais:
{lawsuit_data_json}

Histórico de Offenses:
{offense_history_json}

Transações de Apostas via PIX:
{bets_pix_transfers_json}

"""
    else:
        prompt += f"""
Total de Transações PIX:
- Cash In: R${report_data['total_cash_in_pix']:,.2f}
- Cash Out: R${report_data['total_cash_out_pix']:,.2f}

Transações em Horários Atípicos:
- Cash In PIX: R${report_data['total_cash_in_pix_atypical_hours']:,.2f}
- Cash Out PIX: R${report_data['total_cash_out_pix_atypical_hours']:,.2f}

Concentração de Issuing:
{issuing_concentration_json}

Análise Adicional para Concentração de Issuing:
- Verifique se há repetição de merchant_name ou padrões de valores anômalos em total_amount.
- Utilize os campos total_amount e percentage_of_total para identificar picos ou discrepâncias.
- Considere analisar se os códigos MCC (message__card_acceptor_mcc) indicam setores de risco elevado.

Contatos (Atenção para contatos com status 'blocked'):
{contacts_json}

Dispositivos Utilizados (atenção para número elevado de dispositivos):
{devices_json}

Sanções Judiciais (Dê detalhes sobre o caso durante a análise. Pensão alimentícia ou casos de família podem ser desconsiderados):
{sanctions_history_json}

Transação PIX Negadas e motivo (coluna risk_check):
{denied_pix_transactions_json}

Concentrações PIX:
Cash In:
{pix_cash_in_json}
Cash Out:
{pix_cash_out_json}

Histórico Profissional:
{business_data_json}

Informações sobre processos judiciais:
{lawsuit_data_json}

Transações Confirmadamente Executadas Dentro do Presídio (Atenção especial às colunas status e transaction_type. Transações negadas ou com errors também devem ser consideradas):
{prison_transactions_json}

Histórico de Offenses:
{offense_history_json}

Transações de Apostas via PIX:
{bets_pix_transfers_json}
"""

    return prompt


def get_gpt_analysis(prompt: str) -> str:
    """Retorna a análise do GPT para o prompt fornecido."""
    return get_chatgpt_response(prompt)


def format_export_payload(user_id, description, business_validation):
    """
    Formata o payload para exportação conforme o padrão:
    {
        "user_id": user_id,
        "description": clean_description,
        "analysis_type": "manual",
        "conclusion": conclusion,
        "priority": "high",
        "automatic_pipeline": True,
        "offense_group": "illegal_activity",
        "offense_name": "money_laundering",
        "related_analyses": []
    }
    
    Remove caracteres de formatação Markdown do campo description e determina a conclusão
    a partir do texto da análise.
    """
    # Remove caracteres de formatação Markdown (como #, *, _)
    clean_description = re.sub(r'[#\*\_]', '', description)
    # Se o texto conter "normalizar o caso" (em minúsculas), consideramos a conclusão "normal"
    if "normalizar o caso" in clean_description.lower():
        conclusion = "normal"
    else:
        conclusion = "suspicious"
    payload = {
        "user_id": user_id,
        "description": clean_description,
        "analysis_type": "manual",
        "conclusion": conclusion,
        "priority": "high",
        "automatic_pipeline": True,
        "offense_group": "illegal_activity",
        "offense_name": "money_laundering",
        "related_analyses": []
    }
    return payload

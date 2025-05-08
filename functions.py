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
load_dotenv()
logging.basicConfig(level=logging.ERROR)

class CustomJSONEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, decimal.Decimal):
      return float(obj)
    elif isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
      return obj.isoformat()
    else:
      return super().default(obj)

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 40)
pd.set_option('display.min_rows', 40)

project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
location = os.getenv("LOCATION")
client = bigquery.Client(project=project_id, location=location)

def format_date_portuguese(date_str: str) -> str:
  """Formata uma string de data para o formato em português."""
  if date_str is None:
    return 'Not available.'
  month_names = {1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
                 7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"}
  date = datetime.datetime.strptime(date_str, '%d-%m-%Y')
  return f"{date.day} de {month_names[date.month]} de {date.year}"

def format_cpf(cpf: str) -> str:
  """Formata uma string de CPF."""
  if cpf is None:
    return None
  cpf = cpf.replace('.', '').replace('-', '')
  if len(cpf) == 11:
    return f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
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
  return execute_query(query)

def fetch_business_data(user_id: int) -> pd.DataFrame:
  """Busca dados de relacionamento empresarial para o user_id informado."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_business_relationships_data`
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_sanctions_history(user_id: int) -> pd.DataFrame:
  """Busca dados de sanções para o user_id informado."""
  query = f"""
  SELECT * FROM infinitepay-production.metrics_amlft.sanctions_history
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_denied_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações negadas para o user_id (merchant_id)."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_transactions_data`
  WHERE merchant_id = {user_id} ORDER BY card_number
  """
  return execute_query(query)

def fetch_denied_pix_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações PIX negadas para o user_id."""
  query = f"""
  SELECT * FROM `infinitepay-production.metrics_amlft.lavandowski_risk_pix_transfers_data`
  WHERE debitor_user_id = '{user_id}' ORDER BY str_pix_transfer_id DESC
  """
  return execute_query(query)

def fetch_prison_transactions(user_id: int) -> pd.DataFrame:
  """Busca transações no presídio para o user_id informado."""
  query = f"""
  SELECT * EXCEPT(user_id) FROM infinitepay-production.metrics_amlft.prison_transactions
  WHERE user_id = {user_id}
  """
  return execute_query(query)

def fetch_bets_pix_transfers(user_id: int) -> pd.DataFrame:
  """Busca transações de apostas via PIX para o user_id informado."""
  query = f"""
    SELECT
  transfer_type,
  pix_status,
  user_id,
  user_name,
  gateway,
  gateway_document_number,
  gateway_pix_key,
  gateway_name,
  SUM(transfer_amount) total_amount,
  COUNT(pix_transfer_id) count_transactions
FROM `infinitepay-production.metrics_amlft.bets_pix_transfers`
WHERE user_id = {user_id}
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
  """
  return execute_query(query)

def convert_decimals(data):
  """Converte recursivamente objetos Decimal em float."""
  if isinstance(data, list):
    return [{k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v for k, v in item.items()} for item in data]
  elif isinstance(data, dict):
    return {k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v for k, v in data.items()}
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
  if alert_type == 'betting_houses_alert [BR]' and betting_houses is not None:
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com casas de apostas."

Atenção especial para transações com as casas de apostas abaixo:
{betting_houses.to_json(orient='records', force_ascii=False, indent=2)}

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Verificar se o nome da parte ou o CNPJ corresponde a alguma das casas de apostas listadas acima.
2. Se houver correspondência, calcular:
 a) A soma total de valores transacionados com essa casa de apostas específica.
 b) A porcentagem que essa soma representa do valor TOTAL de Cash In ou Cash Out (conforme aplicável).

Na sua análise, descreva:
- A soma total de Cash In e Cash Out para cada casa de apostas correspondente.
- A porcentagem que esses valores representam do total de Cash In e Cash Out.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações, independentemente do tipo de alerta.
"""
  elif alert_type == 'Goverment_Corporate_Cards_Alert':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com cartões corporativos governamentais."

Atenção especial para transações com BINs de cartões de crédito que começam com os seguintes prefixos:
- 409869
- 467481
- 498409

Para CADA transação, você DEVE:
1. Verificar se o BIN (os primeiros 6 dígitos do número do cartão) corresponde a algum dos prefixos listados acima.
2. Se houver correspondência, calcular:
 a) A soma total de valores transacionados com esses BINs específicos.
 b) A porcentagem que essa soma representa do valor de TPV TOTAL (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para cada prefixo BIN correspondente.
- A porcentagem que esses valores representam do total de Cash In e Cash Out.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações de cartões de crédito relacionadas a este alerta.
Se não houver correspondências com os BINs listados, informe explicitamente na sua análise.
"""
  elif alert_type == 'ch_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente com possíveis anomalias em PIX."

Atenção especial para Transações PIX:

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Analisar os valores de Cash In e Cash Out para identificar quaisquer anomalias ou padrões suspeitos.
2. Comparar os valores com transações típicas para determinar se há desvios significativos.

Na sua análise, descreva:
- Quaisquer transações de Cash In ou Cash Out que apresentam valores anormais.
- Padrões ou tendências observadas nas transações PIX.
- Recomendação sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações PIX relacionadas a este alerta.
Se não houver anomalias detectadas, informe explicitamente na sua análise.

Além disso, você deve verificar se o usuário pode ser estrangeiro, quando nome não soar Brasileiro, ou a data de criação do CPF for muito recente.
"""
  elif alert_type == 'pix_merchant_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente Merchant com possíveis anomalias em PIX Cash In."
Atenção especial para Transações PIX Cash-In e Cash-Out:

Para CADA transação em Cash In e Cash Out, você DEVE:
1. Analisar os valores de Cash In para identificar quaisquer anomalias ou padrões suspeitos.
2. Revisar os valores de Cash Out para detectar valores atípicos ou incomuns.

Na sua análise, descreva:
- Quaisquer transações de Cash In que apresentam valores anormais.
- Quaisquer transações de Cash Out que apresentam valores atípicos ou incomuns.
- Padrões ou tendências observadas nas transações PIX Cash-In e Cash-Out.
- Recomendação sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações PIX relacionadas a este alerta.
Se não houver anomalias ou valores atípicos detectados, informe explicitamente na sua análise.
"""
  elif alert_type == 'international_cards_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com cartões internacionais."
Atenção especial para Transações com Issuer Não Brasileiro:

Para CADA transação, você DEVE:
1. Verificar se o nome do emissor (issuer_name) da transação não é de uma instituição financeira brasileira.
2. Se o emissor não for do Brasil, calcular:
 a) A soma total de valores transacionados com esse emissor específico.
 b) A porcentagem que essa soma representa do TPV Total (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para cada emissor não brasileiro correspondente.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver correspondências com emissores não brasileiros, informe explicitamente na sua análise.
"""
  elif alert_type == 'bank_slips_alert [BR]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente com possíveis anomalias envolvendo boletos bancários."

Atenção especial para Transações com Método de Captura 'bank_slip':

Para CADA transação, você DEVE:
1. Verificar se o método de captura (capture_method) da transação é 'bank_slip'.
2. Se for 'bank_slip', analisar:
 a) A soma total de valores transacionados com este método.
 b) A porcentagem que essa soma representa do valor do TPV TOTAL (conforme aplicável).

Na sua análise, descreva:
- A soma total de valores para transações capturadas via 'bank_slip'.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver transações com método de captura 'bank_slip', informe explicitamente na sua análise.
"""
  elif alert_type == 'gafi_alert [US]':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando com países proibidos do GAFI."

Atenção especial para Transações cujo issuer seja emitido em algum dos países abaixo:

'Bulgaria', 'Burkina Faso', 'Cameroon', 'Croatia', 'Haiti', 'Jamaica', 'Kenya', 'Mali', 'Mozambique',
'Myanmar', 'Namibia', 'Nigeria', 'Philippines', 'Senegal', 'South Africa', 'Tanzania', 'Vietnam', 'Congo, Dem. Rep.',
'Syrian Arab Republic', 'Turkey', 'Yemen, Rep.', 'Yemen Democratic', 'Iran, Islamic Rep.', 'Korea, Dem. Rep.' ,'Venezuela'

Para CADA transação, você DEVE:
1. Verificar se o nome do emissor (issuer_name) da transação não é de alguma instituição financeira com oriens em algum dos países acima.
2. Se positivo, calcular:
 a) A soma total de valores transacionados com esse emissor específico.
 b) A porcentagem que essa soma representa do TPV Total (conforme aplicável).
 c) Nomear o país de origem.

Na sua análise, descreva:
- A soma total de valores para cada emissor com origens nos países acima, restritos pelo GAFI.
- A porcentagem que esses valores representam do TPV total.
- Discuta quaisquer padrões ou anomalias observados nessas transações.

Lembre-se: Esta verificação deve ser feita para TODAS as transações relacionadas a este alerta.
Se não houver correspondências com emissores não brasileiros, informe explicitamente na sua análise.
"""
  elif alert_type == 'Pep_Pix Alert' and pep_data is not None:
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente transacionando com Pessoas Politicamente Expostas (PEP)."

Atenção especial para as transações identificadas abaixo:
{pep_data.to_json(orient='records', force_ascii=False, indent=2)}

Você DEVE:
1. Para cada PEP na lista, informar:
 - Nome completo do PEP (pep_name)
 - Documento do PEP (pep_document_number).
 - Cargo do PEP (job_description).
 - Órgão de trabalho (agencies).
 - Soma total dos valores transacionados com cada PEP (DEBIT + CREDIT).
 - A porcentagem que essa soma representa do total de Cash In e/ou Cash Out transacionado com outros indivíduos.
2. Analisar se os valores e frequências das transações com PEP são atípicos ou suspeitos.

Na sua análise, descreva:
- Detalhes das transações com cada PEP identificado.
- Qualquer padrão ou anomalia observada nessas transações.
- Recomendações sobre a necessidade de investigação adicional com base nos achados.

Lembre-se: Esta verificação deve ser feita para TODAS as transações de Cash In e Cash Out relacionadas a este alerta.
"""
  elif alert_type == 'AI Alert' and features:
    prompt += f"""
Atenção especial às anomalias identificadas pelo modelo de AI:
{features}

Por favor, descreva os padrões ou comportamentos anômalos identificados com base nas características acima.
Você também deve analisar os demais dados disponíveis, como transações, contatos, dispositivos, issuing, produtos, para confirmar ou ajustar a suspeita de fraude.
"""
  elif alert_type == 'Issuing Transactions Alert':
    prompt += f"""
A primeira frase da sua análise deve ser: "Cliente está transacionando altos valores via Issuing."

Atenção especial para a tabela de Issuing e as seguintes informações:
- Coluna total_amount
- mcc e mcc_description
- card_acceptor_country_code

Na sua análise, descreva:
- merchant_name com total_amount e percentage_of_total elevados.
- Se mcc e mcc_description fazem parte de negócios de alto risco.
- Se o país em card_acceptor_country_code é considerado um país de alto risco.
"""
  prompt += """

Importante - Ao final da sua análise, você DEVE incluir uma classificação de risco de lavagem de dinheiro em uma escala de 1 a 10, seguindo estas diretrizes:

- 1 a 5: Baixo risco (Normal - não exige ação adicional)
- 6: Médio risco (Normal com aviso de monitoramento)
- 7 a 8: Médio-Alto risco (Suspicious Mid - requer verificação)
- 9: Alto risco (Suspicious High - requer Business Validation urgente - BV)
- 10: Risco extremo (Offense High - requer descredenciamento e reporte ao COAF)

Fatores para considerar na classificação de risco:
- Volume e frequência de transações
- Presença em listas restritivas ou processos
- Conexões com PEPs
- Transações em horários atípicos
- Transações com países de alto risco
- Compatibilidade entre perfil declarado e comportamento transacional

Formato: "Risco de Lavagem de Dinheiro: X/10" (onde X é o número de 1 a 10)
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
  clean_description = re.sub(r'[#\*\_]', '', description)
  
  # Verifica se há mensagens de erro na descrição
  error_indicators = [
    "Não consigo tankar este caso", 
    "An error occurred",
    "muitas transações",
    "context_length_exceeded",
    "token limit",
    "chame um analista humano"
  ]
  
  has_error = any(indicator.lower() in clean_description.lower() for indicator in error_indicators)
  
  if has_error:
    # Se houver erro, deixa a conclusão vazia para não enviar nem "suspicious" nem "normal"
    conclusion = ""
  else:
    risk_score = 0
    risk_score_match = re.search(r'Risco de Lavagem de Dinheiro: (\d+)/10', clean_description)
    if risk_score_match:
      risk_score = int(risk_score_match.group(1))
    
    # Nova lógica de classificação baseada no score
    if risk_score <= 5:
      # Baixo risco (1-5): normal
      conclusion = "normal"
    elif risk_score <= 6:
      # Médio risco (6): normal com aviso
      conclusion = "normal"
      # Adicionar texto de aviso ao final da descrição
      if not "Caso de médio risco" in clean_description:
        clean_description += "\n\nOBS: Caso de médio risco que deve ser monitorado."
    elif risk_score <= 8:
      # Risco médio-alto (7-8): suspicious mid
      conclusion = "suspicious"
      if not "Caso de risco médio-alto" in clean_description:
        clean_description += "\n\nOBS: Caso de risco médio-alto que requer atenção (suspicious mid)."
      payload = {
        "user_id": user_id,
        "description": clean_description,
        "analysis_type": "manual",
        "conclusion": conclusion,
        "priority": "mid",
        "automatic_pipeline": True,
        "offense_group": "illegal_activity",
        "offense_name": "money_laundering",
        "related_analyses": []
      }
      return payload
    elif risk_score <= 9:
      # Alto risco (9): suspicious high
      conclusion = "suspicious"
    else:
      # Risco extremo (10): offense high
      conclusion = "offense"
    
    # Se explicitamente mencionar normalizar o caso, mantem como normal
    if "normalizar o caso" in clean_description.lower() and conclusion != "offense":
      conclusion = "normal"
  
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

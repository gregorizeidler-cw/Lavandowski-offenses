import datetime
import pandas as pd
from google.cloud import bigquery
from gpt_utils import get_chatgpt_response
import json
import decimal
import logging
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.ERROR)

# Create a custom JSON encoder to handle Decimal objectsclass CustomJSONEncoder(json.JSONEncoder):
class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        elif isinstance(obj, (pd.Timestamp, datetime.datetime, datetime.date)):
            return obj.isoformat()
        else:
            return super().default(obj)

# Set up Pandas options
pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', 40)
pd.set_option('display.min_rows', 40)

# Connect to Google Cloud project
project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
location = os.getenv("LOCATION")
client = bigquery.Client(project=project_id, location=location)

def format_date_portuguese(date_str: str) -> str:
    """Formats a date string to Portuguese date format."""
    if date_str is None:
        return 'Not available.'
    month_names = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril", 5: "Maio", 6: "Junho",
        7: "Julho", 8: "Agosto", 9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    date = datetime.datetime.strptime(date_str, '%d-%m-%Y')
    return f"{date.day} de {month_names[date.month]} de {date.year}"

def format_cpf(cpf: str) -> str:
    """Formats a CPF string."""
    if cpf is None:
        return None
    cpf = cpf.replace('.', '').replace('-', '')
    if len(cpf) == 11:
        formatted_cpf = f"{cpf[:3]}.{cpf[3:6]}.{cpf[6:9]}-{cpf[9:11]}"
        return formatted_cpf
    else:
        return cpf

def fetch_lawsuit_data(user_id: int) -> pd.DataFrame:
    """Fetches lawsuit data for the given user_id."""
    query = f"""
    WITH client AS (
        SELECT 
            u.id AS user_id,
            CASE WHEN m.user_id IS NULL THEN NULL ELSE m.document_number END AS document_number,
            CASE WHEN m.user_id IS NULL THEN c.cpf ELSE lr.cpf END AS cpf,
            CASE WHEN m.user_id IS NULL THEN u.status ELSE m.status END AS status,
            CASE WHEN m.user_id IS NULL THEN u.status_reason ELSE m.status_reason END AS status_reason,
            CASE WHEN m.user_id IS NULL THEN c.service ELSE m.service END AS service,
            CASE WHEN m.user_id IS NULL THEN c.risk_level ELSE m.risk_level END AS risk_level
        FROM `infinitepay-production.maindb.users` AS u
        LEFT JOIN `infinitepay-production.maindb.cardholders` AS c ON c.user_id = u.id
        LEFT JOIN `infinitepay-production.maindb.merchants` AS m ON m.user_id = u.id
        LEFT JOIN `infinitepay-production.maindb.legal_representatives` AS lr ON lr.id = m.legal_representative_id
    ),
    
    offensors AS (
        SELECT 
            c.user_id, 
            c.document_number, 
            c.cpf, 
            o.name AS offense_name, 
            an.conclusion, 
            an.priority, 
            an.description, 
            an.created_at, 
            ROW_NUMBER() OVER (PARTITION BY an.user_id ORDER BY an.created_at DESC) AS rownum
        FROM client AS c
        INNER JOIN `infinitepay-production.maindb.offense_analyses` AS an USING (user_id)
        INNER JOIN `infinitepay-production.maindb.offenses` AS o ON o.id = an.offense_id
        WHERE (c.status = 'approved' OR c.status = 'active')
          AND c.status_reason = 'risk_recheck'
          AND c.service = 'infinitepay'
          AND c.risk_level = 3
          AND o.offense_group = 'affiliation_management'
          AND o.name NOT IN ('mcc_inconsistency', 'missing_data', 'cnpj_closed', 'cnpj_inactive', 'cpf_not_regular', 'invalid_qsa', 'account_closure')
    ),
    
    kyc AS (
        SELECT 
            document_number, 
            lawsuits, 
            KycData, 
            ROW_NUMBER() OVER (PARTITION BY document_number ORDER BY created_at DESC) AS rownum
        FROM `infinitepay-production.maindb.kyc`
        WHERE source = 'bigdata_recheck'
          AND (lawsuits IS NOT NULL OR KycData IS NOT NULL)
    ),
    
    lawsuits AS (
        SELECT DISTINCT 
            o.user_id,
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) AS MainSubject,
            JSON_EXTRACT_SCALAR(Parties, '$.Polarity') AS party_polarity,
            JSON_EXTRACT_SCALAR(Parties, '$.Type') AS party_type,
            JSON_EXTRACT_SCALAR(Parties, '$.PartyDetails.SpecificType') AS specific_party_type,
            JSON_EXTRACT_SCALAR(processes, '$.Status') AS lawsuit_status,
            JSON_EXTRACT_SCALAR(processes, '$.Type') AS lawsuit_type
        FROM offensors AS o
        INNER JOIN kyc AS k ON k.document_number = o.document_number OR k.document_number = o.cpf
        CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(k.lawsuits, '$')) AS processes
        CROSS JOIN UNNEST(JSON_EXTRACT_ARRAY(processes, '$.Parties')) AS Parties
        WHERE o.rownum = 1
          AND k.rownum = 1
          AND o.offense_name = 'risk_lawsuit'
          AND (JSON_EXTRACT_SCALAR(Parties, '$.Doc') = o.cpf OR JSON_EXTRACT_SCALAR(Parties, '$.Doc') = o.document_number)
          AND (
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ADULTERACAO DE VEICULO AUTOMOTOR%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%APROPRIACAO INDEBITA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ASSOCIACAO CRIMINOSA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ATENTADO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%AUTOLAVAGEM%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%BRANQUEAMENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CARTEL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CASA DE PROSTITUICAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CLANDESTINO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COAF%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COERCAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COLABORACAO PREMIADA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COMERCIO CLANDESTINO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COMERCIO ILEGAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%COMMINGLING%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CONCUSSAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CONTABILIDADE PARALELA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CONTRABANDO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CORRUPCAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CORRUPCAO ATIVA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CORRUPCAO DE MENORES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CORRUPCAO PASSIVA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CORRUPCAO PASSIVA PRIVILEGIADA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME AMBIENTAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME CONTRA A ADMINISTRACAO DA JUSTICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME CONTRA A ORDEM ECONOMICO-FINANCEIRA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME CONTRA A PAZ PUBLICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME CONTRA AS FINANCAS PUBLICAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME CONTRA O MEIO AMBIENTE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME DE PREFEITOS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME FINANCEIRO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME HEDIONDO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIME ORGANIZADO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES AMBIENTAIS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA A ADMINISTRACAO DA JUSTICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA A ORDEM ECONOMICO-FINANCEIRA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA A PAZ PUBLICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA A SAUDE PUBLICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA AS FINANCAS PUBLICAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA O MEIO AMBIENTE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA O MERCADO DE CAPITAIS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES CONTRA O SISTEMA FINANCEIRO NACIONAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES DO SISTEMA NACIONAL DE ARMAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIMES EQUIPARADOS A HEDIONDOS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIPTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%CRIPTOMOEDAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DECLARACAO FALSA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DELACAO PREMIADA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DESCAMINHO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DESMATAMENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DESMATAMENTO, EXPLORACAO, ECONOMICA OU DEGRADACAO DE FLORESTA EM TERRAS DE DOMINIO PUBLICO OU DEVOLUTAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%DOLAR-CABO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EMPRESA FACHADA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EMPRESA FICTICIA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ESCRAVO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ESTELIONATARIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ESTELIONATO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EURO-CABO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EVASAO DE DIVISAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EVASAO FISCAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EXPLORACAO SEXUAL DE CRIANCA OU ADOLESCENTE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EXPORTACAO IRREGULAR DE PELES E COUROS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%EXTRACAO DE MINERAIS DE FLORESTAS DE DOMINIO PUBLICO OU DE PRESERVACAO PERMANENTE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSA IDENTIDADE PARA REALIZACAO DE OPERACAO DE CAMBIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSA MANIFESTACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADE DOCUMENTAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADE EM DECLARACAO DE CREDITO OU RECLAMACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADE EM DEMONSTRATIVOS CONTABEIS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADE EM TITULO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADE IDEOLOGICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIDADES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FALSIFICACAO DE DOCUMENTO PUBLICO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FATF%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FAVORECIMENTO DA PROSTITUICAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FEMINICIDIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FINANCIAMENTO AO TERRORISMO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FINANCIAMENTO OU CUSTEIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDAR%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE A LICITACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE DE CONCORRENCIA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE EM ARREMATACAO JUDICIAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE NA OBTENCAO DE FINANCIAMENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDE PROCESSUAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDOU%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDULENTA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDULENTAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRAUDULENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FRUSTRACAO OU FRAUDE DO CARATER COMPETITIVO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FUGA DE PESSOA PRESA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%FURTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%GAFI%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%GENOCIDIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%HEDIONDO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%HOMICIDIO QUALIFICADO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%IMPEDIMENTO, PERTURBACAO OU FRAUDE A ATO LICITATORIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INFILTRACAO DE AGENTES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INFILTRACAO POLICIAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTEGRACAO OU UTILIZACAO DE VALORES SUSPEITOS EM ATIVIDADE ECONOMICA OU FINANCEIRA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTEGRATION%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTERCEPTACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTERCEPTACAO TELEFONICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTRODUCAO DE APARELHO DE COMUNICACAO EM ESTABELECIMENTO PRISIONAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INTRODUCAO IRREGULAR DE ESPECIME ANIMAL NO PAIS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%INVESTIGACAO CRIMINAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LATROCINIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LAVAGEM DE CAPITAIS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LAVAGEM DE DINHEIRO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LAVRA SEM AUTORIZACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LAYERING%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%LCSFN%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%MANIPULACAO DO MERCADO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%MODIFICACAO ILEGAL DO CONTRATO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%MODIFICACAO OU ALTERACAO NAO AUTORIZADA DE SISTEMA DE INFORMACOES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%MOEDA FALSA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OBRA OU SERVICO POTENCIALMENTE POLUIDOR SEM LICENCA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OCULTACAO DE BENS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OCULTACAO DE DIREITOS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OCULTACAO DE VALORES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OFFSHORE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OPERACAO DE CAMBIO COM FALSA IDENTIDADE E PRESTACAO DE INFORMACAO FALSA EM OPERACAO DE CAMBIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%OPERACAO DE CAMBIO COM O FIM DE EVASAO DE DIVISAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ORDEM ECONOMICA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ORGANIZACAO CRIMINOSA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PECULATO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PECULATO CULPOSO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PECULATO-APROPRIACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PECULATO-DESVIO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PECULATO-FURTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PESCA PREDATORIA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PESSOA PRESA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PETRECHOS PARA FALSIFICACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PETRECHOS PARA PREPARACAO, FABRICACAO OU TRANSFORMACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PLACEMENT%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PORNOGRAFIA INFANTIL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PORTE ILEGAL DE ARMA DE FOGO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%POSSE ILEGAL DE ARMA DE FOGO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PRESTACAO DE INFORMACAO FALSA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PREVARICACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PRISAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%PRISAO EM FLAGRANTE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%QUADRILHA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%RECEPTACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%RELATORIO DE INTELIGENCIA FINANCEIRA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%REMESSA CLANDESTINA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%REMESSA FRAUDULENTA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%ROUBO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SELFLAUNDERING%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SLUSH FUND%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SMURFING%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SOBREFATURAMENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SONEGACAO E FRAUDE%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SUBFATURAMENTO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SUPERFATURAMENTO OU FRAUDE NA LICITACAO OU NA EXECUCAO DO CONTRATO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%SUPERFATURAMENTO OU SUBFATURAMENTO EM IMPORTACAO OU EXPORTACAO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TERRORISMO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TERRORISTA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TORTURA DO ENCARCERADO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TORTURA-CASTIGO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRABALHO ESCRAVO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRABALHO INFANTIL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO DE DROGAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO DE INFLUENCIA%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO DE PESSOAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO ILICITO DE DROGAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO ILICITO E USO INDEVIDO DE DROGAS%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO INTERNACIONAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO INTERNACIONAL DE CRIANCAS E ADOLESCENTES%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%TRAFICO TRANSNACIONAL%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%USO DE DOCUMENTO FALSO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%VENDER/EXPOR A VENDA FOTO/VIDEO/OUTRO REGISTRO COM CENA SEXO EXPLICITO%' OR
            UPPER(JSON_EXTRACT_SCALAR(processes, '$.MainSubject')) LIKE '%WHISTLEBLOWER%'
        )
          )
    
    SELECT * FROM lawsuits
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df

def fetch_business_data(user_id: int) -> pd.DataFrame:
    """Fetches business data for the given user_id."""
    query = f"""
    WITH User_Document AS (
    SELECT
        id AS user_id,
        REGEXP_REPLACE(CAST(cpf AS STRING), r'[^0-9]', '') AS document_number
    FROM maindb.cardholders
    WHERE id = {user_id}

    UNION ALL

    SELECT
        user_id,
        REGEXP_REPLACE(CAST(document_number AS STRING), r'[^0-9]', '') AS document_number
    FROM maindb.merchants
    WHERE user_id = {user_id}
    ),

    Filtered_KYC AS (
    SELECT
        document_number,
        BusinessRelationships
    FROM maindb.kyc
    WHERE REGEXP_REPLACE(CAST(document_number AS STRING), r'[^0-9]', '') IN (SELECT document_number FROM User_Document)
    ),

    UnnestBusinessRelationships AS (
    SELECT
        REGEXP_REPLACE(CAST(document_number AS STRING), r'[^0-9]', '') AS document_number,
        BR
    FROM Filtered_KYC,
    UNNEST(JSON_EXTRACT_ARRAY(BusinessRelationships)) AS BR
    )

    SELECT
    u.user_id,
    ubr.document_number,
    JSON_VALUE(ubr.BR, '$.RelatedEntityTaxIdNumber') AS RelatedEntityTaxIdNumber,
    JSON_VALUE(ubr.BR, '$.RelatedEntityName') AS RelatedEntityName,
    JSON_VALUE(ubr.BR, '$.RelatedEntityTaxIdCountry') AS Country,
    JSON_VALUE(ubr.BR, '$.RelationshipName') AS RelationshipName,
    JSON_VALUE(ubr.BR, '$.RelationshipType') AS RelationshipType,
    JSON_VALUE(ubr.BR, '$.RelationshipSubtype') AS RelationshipSubtype,
    JSON_VALUE(ubr.BR, '$.IsCurrentlyActive') AS IsCurrentlyActive,
    JSON_VALUE(ubr.BR, '$.LastUpdateDate') AS LastUpdateDate
    FROM UnnestBusinessRelationships AS ubr
    JOIN User_Document AS u ON u.document_number = ubr.document_number;
    """

    df = execute_query(query)
    return df

def fetch_sanctions_history(user_id: int) -> pd.DataFrame:
    """Fetches KYC sanctions history for the given user_id."""
    query = f"""
    SELECT * FROM `infinitepay-production.metrics_amlft.sanctions_history` 
WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df

def fetch_denied_transactions(user_id: int) -> pd.DataFrame:
    """Fetches denied transactions for the given user_id (merchant_id)."""
    query = f"""
        SELECT 
        card_number, 
        COUNT(*) AS transaction_count,
        SUM(amount) AS total_amount,
        merchant_id,
        payment_method,
        capture_method,
        card_holder_name,
        action,
        type_of
    FROM `maindb.risk_transactions` 
    WHERE merchant_id = {user_id}
    GROUP BY 
        card_number,
        merchant_id,
        payment_method,
        card_holder_name,
        capture_method,
        action,
        type_of
    ORDER BY 
        card_number;
    """
    df = execute_query(query)
    return df
def fetch_denied_pix_transactions(user_id: int) -> pd.DataFrame:
    """Fetches denied PIX transactions for the given user_id and the risk check that failed."""
    query = f"""
    WITH shadow_rules AS (
      SELECT
        id,
        results_unnested.key AS risk_check
      FROM
        `infinitepay-production.ldg_app_sync.risk_pix_transfers` a,
        UNNEST(a.results) AS results_unnested,
        UNNEST(results_unnested.value) AS result_key_value
      WHERE
        (_PARTITIONTIME >= CURRENT_TIMESTAMP - INTERVAL 15 DAY OR _PARTITIONTIME IS NULL)
        AND result_key_value.key = 'shadow_rule'
        AND result_key_value.value = 'True'
    )
    SELECT
      TIMESTAMP(a.timestamp) AS timestamp,
      a.id AS str_pix_transfer_id,
      a.source_id AS debitor_user_id,
      results_unnested.key AS risk_check,
      result_key_value.value AS individual_risk_check_result
    FROM
      `infinitepay-production.ldg_app_sync.risk_pix_transfers` a,
      UNNEST(a.results) AS results_unnested,
      UNNEST(results_unnested.value) AS result_key_value
    LEFT JOIN
      shadow_rules sr ON (sr.id = a.id AND sr.risk_check = results_unnested.key)
    WHERE
      (_PARTITIONTIME >= CURRENT_TIMESTAMP - INTERVAL 15 DAY OR _PARTITIONTIME IS NULL)
      AND result_key_value.key != 'shadow_rule'
      AND sr.id IS NULL
      AND a.approved = False
      AND a.source_id = {user_id}
      AND result_key_value.value = "False"
    ORDER BY
      a.id DESC;
    """
    df = execute_query(query)
    return df

def convert_decimals(data):
    """Recursively converts decimal.Decimal objects to float in data."""
    if isinstance(data, list):
        return [
            {
                k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v
                for k, v in item.items()
            }
            for item in data
        ]
    elif isinstance(data, dict):
        return {
            k: float(v) if isinstance(v, (decimal.Decimal, float, int)) else v
            for k, v in data.items()
        }
    else:
        return data
    
def fetch_prison_transactions(user_id: int) -> pd.DataFrame:
    """Fetches prison transactions for the given user_id."""
    query = f"""
    SELECT * EXCEPT(user_id) FROM `infinitepay-production.metrics_amlft.prison_transactions`
    WHERE user_id = {user_id}
    """
    df = execute_query(query)
    return df

def execute_query(query):
    """Executes a BigQuery SQL query and returns a DataFrame."""
    try:
        df = client.query(query).result().to_dataframe()
        return df
    except Exception as e:
        logging.error(f"Error executing query: {e}")
        return pd.DataFrame()

def merchant_report(user_id: int, alert_type: str, pep_data=None) -> dict:
    """Generates a report for a merchant user."""
    # Define queries
    query_merchants = f"""
    SELECT * FROM `metrics_amlft.merchant_report`
    WHERE user_id = {user_id}
    LIMIT 1
    """

    query_issuing_concentration = f"""
        WITH processed_payments AS (
        SELECT 
            c.user_id,
            ip.merchant_name,
            SUM(ipa.amount / 100) AS total_amount,
            CAST(ip.mcc AS INT64) AS mcc, -- Casting mcc to INT64
            JSON_EXTRACT_SCALAR(
                ipm.message,
                '$.content.authorization.de43_card_acceptor_name_location.sf5_card_acceptor_state_or_country_code'
            ) AS card_acceptor_country_code
        FROM `maindb.issuing_payments` AS ip
        INNER JOIN `maindb.issuing_payment_messages` AS ipm ON ipm.issuing_payment_id = ip.id
        INNER JOIN `maindb.issuing_payment_actions` AS ipa ON ipm.id = ipa.issuing_payment_message_id
        INNER JOIN `maindb.card_tokens` AS ct ON ct.id = ip.card_token_pan_id
        INNER JOIN `maindb.cardholders` AS c ON c.id = ct.cardholder_id
        WHERE ipa.status = 'processed' 
        AND ipa.type = 'authorize'
        AND c.user_id = {user_id}
        GROUP BY 
            c.user_id, 
            ip.merchant_name, 
            mcc, 
            card_acceptor_country_code
        HAVING SUM(ipa.amount / 100) > 0
    ),
    total_amount AS (
        SELECT 
            user_id,
            SUM(total_amount) AS grand_total
        FROM processed_payments
        GROUP BY user_id
    )
    SELECT 
        pp.user_id,
        pp.merchant_name,
        pp.total_amount,
        pp.mcc,
        mc.edited_description AS mcc_description,
        pp.card_acceptor_country_code,
        CASE
            WHEN ta.grand_total > 0 THEN (pp.total_amount / ta.grand_total) * 100
            ELSE 0
        END AS percentage_of_total
    FROM processed_payments pp
    INNER JOIN total_amount ta ON pp.user_id = ta.user_id
    LEFT JOIN `infinitepay-production.external_sources.mcc_codes` mc ON pp.mcc = mc.mcc
    """

    query_pix_concentration = f"""
    SELECT * FROM `metrics_amlft.pix_concentration`
    WHERE user_id = {user_id}
    """

    query_offense_history = f"""
    SELECT DISTINCT(an.id),
        FORMAT_DATETIME('%d-%m-%Y', an.created_at) AS date,
        FORMAT_DATETIME('%H:%M:%S', an.created_at) AS time,
        an.user_id,
        analysis_type,
        conclusion,
        priority,
        o.offense_group,
        o.name,
        an.description,
        INITCAP(REPLACE(REPLACE(SPLIT(u.email, '@')[OFFSET(0)], '_', ' '), '.', ' ')) AS analyst,
        an.analyst_id,
        an.automatic_pipeline
    FROM `infinitepay-production.maindb.offense_analyses` an
    JOIN `infinitepay-production.maindb.users` u ON u.id = an.analyst_id
    JOIN `infinitepay-production.maindb.offenses` AS o ON o.id = an.offense_id
    LEFT JOIN `infinitepay-production.maindb.offense_actions` AS act ON act.offense_analysis_id = an.id
    WHERE an.user_id = {user_id}
    ORDER BY id DESC;
    """

    query_transaction_concentration = f"""
    SELECT * EXCEPT(merchant_id) FROM `metrics_amlft.cardholder_concentration`
    WHERE merchant_id = {user_id}
    ORDER BY total_approved_by_ch DESC
    """

    products_online_store = f"""
    WITH user_handle AS (
        SELECT handle
        FROM maindb.users
        WHERE id = {user_id}
    ),
    products_filtered AS (
        SELECT
            id,
            name,
            quantity,
            updated_at,
            external_payment_url,
            product_type,
            sales_channels,
            REGEXP_SUBSTR(external_payment_url, 'io/(.*?)/') AS handle
        FROM infinitepay-production.pdvdb.products
        WHERE external_payment_url IS NOT NULL
    )
    SELECT
        p.id,
        p.name,
        p.quantity,
        p.updated_at,
        p.external_payment_url,
        p.product_type,
        p.sales_channels
    FROM products_filtered p
    JOIN user_handle u ON p.handle = u.handle
    WHERE p.external_payment_url IS NOT NULL;
    """

    contacts_query = f"""
    SELECT DISTINCT p.phone_number IS NOT NULL AS has_phonecast,
    u.id AS user_id, name, c.raw_phone_number AS raw_phone_number,
    u.status AS status
    FROM `ai-services-sae.tyrell_euler.scd_raw__contacts` c
    LEFT JOIN infinitepay-production.external_sources.phonecast p 
    ON p.phone_number = `infinitepay-production.udfs.phone_number_norm`(raw_phone_number)
    INNER JOIN infinitepay-production.maindb.users u 
    ON u.phone_number = `infinitepay-production.udfs.phone_number_norm`(raw_phone_number)
    WHERE user_id = {user_id}
    """

    devices_query = f"""
    SELECT * EXCEPT(user_id) FROM `metrics_amlft.user_device`
    WHERE user_id = {user_id}
    """


    # Initialize variables
    merchant_info = pd.DataFrame()
    issuing_concentration = pd.DataFrame()
    pix_concentration = pd.DataFrame()
    offense_history = pd.DataFrame()
    transaction_concentration = pd.DataFrame()
    products_online = pd.DataFrame()
    contacts = pd.DataFrame()
    devices = pd.DataFrame()
    cash_in = pd.DataFrame()
    cash_out = pd.DataFrame()
    total_cash_in_pix = 0.0
    total_cash_out_pix = 0.0
    total_cash_in_pix_atypical_hours = 0.0
    total_cash_out_pix_atypical_hours = 0.0

    # Execute queries
    merchant_info = execute_query(query_merchants)
    issuing_concentration = execute_query(query_issuing_concentration)
    pix_concentration = execute_query(query_pix_concentration)
    offense_history = execute_query(query_offense_history)
    transaction_concentration = execute_query(query_transaction_concentration)
    products_online = execute_query(products_online_store)
    contacts = execute_query(contacts_query)
    devices = execute_query(devices_query)

    # Process PIX concentrations
    if not pix_concentration.empty:
        cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
        cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
        total_cash_in_pix = cash_in['pix_amount'].sum()
        total_cash_out_pix = cash_out['pix_amount'].sum()
        total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
        total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()


    # Convert DataFrames to dictionaries
    merchant_info_dict = merchant_info.to_dict(orient='records')[0] if not merchant_info.empty else {}
    issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
    cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
    cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
    offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
    transaction_concentration_list = transaction_concentration.to_dict(orient='records') if not transaction_concentration.empty else []
    products_online_list = products_online.to_dict(orient='records') if not products_online.empty else []
    contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
    devices_list = devices.to_dict(orient='records') if not devices.empty else []

    # Convert decimals
    merchant_info_dict = convert_decimals(merchant_info_dict)
    issuing_concentration_list = convert_decimals(issuing_concentration_list)
    cash_in_list = convert_decimals(cash_in_list)
    cash_out_list = convert_decimals(cash_out_list)
    offense_history_list = convert_decimals(offense_history_list)
    transaction_concentration_list = convert_decimals(transaction_concentration_list)
    products_online_list = convert_decimals(products_online_list)
    contacts_list = convert_decimals(contacts_list)
    devices_list = convert_decimals(devices_list)

    # Fetch lawsuit data
    lawsuit_data = fetch_lawsuit_data(user_id)
    lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []

    # Fetch denied transactions
    denied_transactions_df = fetch_denied_transactions(user_id)
    denied_transactions_list = denied_transactions_df.to_dict(orient='records') if not denied_transactions_df.empty else []

    # Fetch business data
    business_data = fetch_business_data(user_id)
    business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []

     # Fetch prison transactions
    prison_transactions_df = fetch_prison_transactions(user_id)
    prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []

     # Convert decimals
    prison_transactions_list = convert_decimals(prison_transactions_list)

     # Fetch KYC sanctions history data
    sanctions_history_df = fetch_sanctions_history(user_id)
    sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []

    # Convert decimals if necessary (assuming sanctions history data might contain decimals)
    sanctions_history_list = convert_decimals(sanctions_history_list)

    # Fetch denied PIX transactions
    denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
    denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []

    # Convert decimals if necessary
    denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)

    # Create report dictionary
    report = {
        "merchant_info": merchant_info_dict,
        "total_cash_in_pix": total_cash_in_pix,
        "total_cash_out_pix": total_cash_out_pix,
        "total_cash_in_pix_atypical_hours": total_cash_in_pix_atypical_hours,
        "total_cash_out_pix_atypical_hours": total_cash_out_pix_atypical_hours,
        "issuing_concentration": issuing_concentration_list,
        "pix_cash_in": cash_in_list,
        "pix_cash_out": cash_out_list,
        "offense_history": offense_history_list,
        "transaction_concentration": transaction_concentration_list,
        "products_online": products_online_list,
        "contacts": contacts_list,
        "devices": devices_list,
        "lawsuit_data": lawsuit_data,
        "denied_transactions": denied_transactions_list,
        "business_data": business_data_list,
        "prison_transactions": prison_transactions_list,
        "sanctions_history": sanctions_history_list,
        "denied_pix_transactions": denied_pix_transactions_list
    }

    return report
            
def cardholder_report(user_id: int, alert_type: str, pep_data=None) -> dict:
    """Generates a report for a cardholder user."""
    # Define queries
    query_cardholders = f"""
    SELECT * FROM `metrics_amlft.cardholder_report`
    WHERE user_id = {user_id}
    LIMIT 1
    """

    query_issuing_concentration = f"""
    SELECT * EXCEPT(user_id) FROM `metrics_amlft.issuing_concentration`
    WHERE user_id = {user_id}
    """

    query_pix_concentration = f"""
    SELECT * FROM `metrics_amlft.pix_concentration`
    WHERE user_id = {user_id}
    """

    query_offense_history = f"""
    SELECT DISTINCT(an.id),
        FORMAT_DATETIME('%d-%m-%Y', an.created_at) AS date,
        FORMAT_DATETIME('%H:%M:%S', an.created_at) AS time,
        an.user_id,
        analysis_type,
        conclusion,
        priority,
        o.offense_group,
        o.name,
        an.description,
        INITCAP(REPLACE(REPLACE(SPLIT(u.email, '@')[OFFSET(0)], '_', ' '), '.', ' ')) AS analyst,
        an.analyst_id,
        an.automatic_pipeline
    FROM `infinitepay-production.maindb.offense_analyses` an
    JOIN `infinitepay-production.maindb.users` u ON u.id = an.analyst_id
    JOIN `infinitepay-production.maindb.offenses` AS o ON o.id = an.offense_id
    LEFT JOIN `infinitepay-production.maindb.offense_actions` AS act ON act.offense_analysis_id = an.id
    WHERE an.user_id = {user_id}
    ORDER BY id DESC;
    """

    contacts_query = f"""
    SELECT DISTINCT p.phone_number IS NOT NULL AS has_phonecast,
    u.id AS user_id, name, c.raw_phone_number AS raw_phone_number,
    u.status AS status
    FROM `ai-services-sae.tyrell_euler.scd_raw__contacts` c
    LEFT JOIN infinitepay-production.external_sources.phonecast p 
    ON p.phone_number = `infinitepay-production.udfs.phone_number_norm`(raw_phone_number)
    INNER JOIN infinitepay-production.maindb.users u 
    ON u.phone_number = `infinitepay-production.udfs.phone_number_norm`(raw_phone_number)
    WHERE user_id = {user_id}
    """

    devices_query = f"""
    SELECT
        identity_validation_status,
        ip,
        mdl.created_at,
        model,
        advertising_id,
        platform_id,
        authorized
    FROM maindb.mobile_device_logins mdl
    LEFT JOIN maindb.mobile_devices mb ON mb.id = mdl.mobile_device_id
    WHERE user_id = {user_id}
    """

    # Initialize variables
    cardholder_info = pd.DataFrame()
    issuing_concentration = pd.DataFrame()
    pix_concentration = pd.DataFrame()
    offense_history = pd.DataFrame()
    contacts = pd.DataFrame()
    devices = pd.DataFrame()
    cash_in = pd.DataFrame()
    cash_out = pd.DataFrame()
    total_cash_in_pix = 0.0
    total_cash_out_pix = 0.0
    total_cash_in_pix_atypical_hours = 0.0
    total_cash_out_pix_atypical_hours = 0.0

    # Execute queries
    cardholder_info = execute_query(query_cardholders)
    issuing_concentration = execute_query(query_issuing_concentration)
    pix_concentration = execute_query(query_pix_concentration)
    offense_history = execute_query(query_offense_history)
    contacts = execute_query(contacts_query)
    devices = execute_query(devices_query)

    # Process PIX concentrations
    if not pix_concentration.empty:
        cash_in = pix_concentration[pix_concentration['transaction_type'] == 'Cash In'].round(2)
        cash_out = pix_concentration[pix_concentration['transaction_type'] == 'Cash Out'].round(2)
        total_cash_in_pix = cash_in['pix_amount'].sum()
        total_cash_out_pix = cash_out['pix_amount'].sum()
        total_cash_in_pix_atypical_hours = cash_in['pix_amount_atypical_hours'].sum()
        total_cash_out_pix_atypical_hours = cash_out['pix_amount_atypical_hours'].sum()

            
    # Convert DataFrames to dictionaries
    cardholder_info_dict = cardholder_info.to_dict(orient='records')[0] if not cardholder_info.empty else {}
    issuing_concentration_list = issuing_concentration.to_dict(orient='records') if not issuing_concentration.empty else []
    cash_in_list = cash_in.to_dict(orient='records') if not cash_in.empty else []
    cash_out_list = cash_out.to_dict(orient='records') if not cash_out.empty else []
    offense_history_list = offense_history.to_dict(orient='records') if not offense_history.empty else []
    contacts_list = contacts.to_dict(orient='records') if not contacts.empty else []
    devices_list = devices.to_dict(orient='records') if not devices.empty else []

    # Convert decimals
    cardholder_info_dict = convert_decimals(cardholder_info_dict)
    issuing_concentration_list = convert_decimals(issuing_concentration_list)
    cash_in_list = convert_decimals(cash_in_list)
    cash_out_list = convert_decimals(cash_out_list)
    offense_history_list = convert_decimals(offense_history_list)
    contacts_list = convert_decimals(contacts_list)
    devices_list = convert_decimals(devices_list)

    # Fetch lawsuit data
    lawsuit_data = fetch_lawsuit_data(user_id)
    lawsuit_data = lawsuit_data.to_dict(orient='records') if not lawsuit_data.empty else []

    # Fetch business data
    business_data = fetch_business_data(user_id)
    business_data_list = business_data.to_dict(orient='records') if not business_data.empty else []

    # Fetch prison transactions
    prison_transactions_df = fetch_prison_transactions(user_id)
    prison_transactions_list = prison_transactions_df.to_dict(orient='records') if not prison_transactions_df.empty else []

    # Convert decimals
    prison_transactions_list = convert_decimals(prison_transactions_list)

    # Fetch KYC sanctions history data
    sanctions_history_df = fetch_sanctions_history(user_id)
    sanctions_history_list = sanctions_history_df.to_dict(orient='records') if not sanctions_history_df.empty else []

    # Convert decimals if necessary
    sanctions_history_list = convert_decimals(sanctions_history_list)

    # Fetch denied PIX transactions
    denied_pix_transactions_df = fetch_denied_pix_transactions(user_id)
    denied_pix_transactions_list = denied_pix_transactions_df.to_dict(orient='records') if not denied_pix_transactions_df.empty else []

    # Convert decimals if necessary
    denied_pix_transactions_list = convert_decimals(denied_pix_transactions_list)

    # Create report dictionary
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
        "denied_pix_transactions": denied_pix_transactions_list
    }

    return report

def generate_prompt(report_data: dict, user_type: str, alert_type: str, betting_houses: pd.DataFrame = None, pep_data: pd.DataFrame = None, features: str = None) -> str:
    """Generates the prompt for the GPT model based on the report data."""
    import json

    # Serialize data to JSON strings
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

    # Include data specific to merchants
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
"""
    else:  # For cardholders
        prompt += f"""
Total de Transações PIX:
- Cash In: R${report_data['total_cash_in_pix']:,.2f}
- Cash Out: R${report_data['total_cash_out_pix']:,.2f}

Transações em Horários Atípicos:
- Cash In PIX: R${report_data['total_cash_in_pix_atypical_hours']:,.2f}
- Cash Out PIX: R${report_data['total_cash_out_pix_atypical_hours']:,.2f}

Concentração de Issuing:
{issuing_concentration_json}

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
"""

    # Additional instructions based on alert type
    if alert_type == 'betting_houses_alert [BR]' and betting_houses is not None:
        betting_houses_json = betting_houses.to_json(orient='records', force_ascii=False, indent=2)
        prompt += f"""
            A primeira frase da sua análise deve ser: "Cliente está transacionando com casas de apostas."

            Atenção especial para transações com as casas de apostas abaixo:
            {betting_houses_json}

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
            Se não houver correspondências com casas de apostas, informe explicitamente na sua análise.
            """
    elif alert_type == 'goverment_corporate_cards_alert [BR]':
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
            1. Verificar se o nome do emissor (`issuer_name`) da transação não é de uma instituição financeira brasileira.
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
            1. Verificar se o método de captura (`capture_method`) da transação é 'bank_slip'.
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
            1. Verificar se o nome do emissor (`issuer_name`) da transação não é de alguma instituição financeira com oriens em algum dos países acima.
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

    elif alert_type == 'pep_pix_alert [BR]' and pep_data is not None:
        pep_data_json = pep_data.to_json(orient='records', force_ascii=False, indent=2)
        prompt += f"""
            A primeira frase da sua análise deve ser: "Cliente transacionando com Pessoas Politicamente Expostas (PEP)."

            Atenção especial para as transações identificadas abaixo:
            {pep_data_json}
            
            Você DEVE:
            1. Para cada PEP na lista, informar:
            - Nome completo do PEP (`pep_name`)
            - Documento do PEP (`pep_document_number`).
            - Cargo do PEP (`job_description`).
            - Órgão de trabalho (`agencies`).
            - Soma total dos valores transacionados com cada PEP (DEBIT + CREDIT).
            - A porcentagem que essa soma representa do total de Cash In e/ou Cash Out transacionado com outros indivíduos.
            3. Analisar se os valores e frequências das transações com PEP são atípicos ou suspeitos.

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
        
    return prompt

def get_gpt_analysis(prompt: str) -> str:
    """Gets the GPT analysis for the given prompt.

    Args:
        prompt (str): The prompt to send to the LLM.

    Returns:
        str: The GPT's analysis.
    """
    gpt_analysis = get_chatgpt_response(prompt)
    gpt_analysis = gpt_analysis.replace("R$", "R\\$")
    return gpt_analysis
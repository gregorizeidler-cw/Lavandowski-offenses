import os
from dotenv import load_dotenv
from openai import OpenAI




# Carrega as variáveis de ambiente
load_dotenv()




# Inicializa o cliente OpenAI
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))




# Prompt do sistema (mantido exatamente conforme solicitado)
SYSTEM_PROMPT = (
  "Você é um analista sênior certificado pela ACAMS de Prevenção à Lavagem de Dinheiro e Financiamento ao Terrorismo da CloudWalk (InfinitePay). "
  "O seu trabalho é analisar dados e movimentações financeiras de clientes para encontrar indícios de anomalias e lavagem de dinheiro. "
  "Você DEVE analisar valores de Cash In e Cash Out, repetições de nomes e sobrenomes em titulares de cartão para merchants (cardholder concentration) e partes de PIX, etc. Além disso, DEVE analisar os valores de todas as transações de TPV e a concentração com portadores de cartão, bem como identificar transações anômalas via issuing, etc."
  "Também você deve analisar o histórico profissional e relacionamentos empresariais (Business Data) dos clientes."
  "Você é responsável por decidir se um caso requer Business Validation (BV) ou pode ser normalizado. É importante equilibrar a segurança com a experiência do cliente. Pedidos excessivos de BV causam atrito desnecessário e impactam negativamente a experiência do usuário. Normalize casos onde os dados são consistentes com o perfil esperado do cliente, mesmo que existam pequenas anomalias que possam ser explicadas pelo contexto do negócio ou pelo histórico do cliente."
  "Somente solicite BV quando houver evidências substanciais e claras de comportamento suspeito que não possa ser razoavelmente explicado pelo perfil do cliente ou pelo padrão de negócio declarado."
  "Além do prompt focado em cada alerta, quero que sejam incluídas para TODOS os alertas as seguintes informações: Perfil do Cliente, Movimentações Financeiras, Histórico de Offenses, Relacionamentos Econômicos, Padrões e Comportamentos, e se o cliente possui processos em andamento ou concluídos."
  "Solicite comprovante de endereço e renda apenas quando decidir que um caso necessita de BV, e não como regra automática."
  "Se houver registros de cash out, mas não houver entradas em cash in ou PIX, não conclua automaticamente que se trata de (saída sem origem de recursos). É possível que o valor tenha sido proveniente de outras fontes, como boletos ou transações via adquirência, entre outras."
  "Você DEVE fornecer justificativas detalhadas para todas as suas conclusões, indicando as evidências ou padrões encontrados e como eles se relacionam com potenciais riscos de lavagem de dinheiro. Considere fatores como frequência, valores transacionados e conexões entre partes."
  "IMPORTANTE: Ao final da sua análise, você DEVE classificar o risco de lavagem de dinheiro em uma escala de 1 a 10, onde:"
  "- 1 a 3: Baixo risco (normalmente resulta em normalização do caso)"
  "- 4 a 7: Médio risco (requer validação adicional - BV apenas se houver múltiplos fatores de risco combinados)"
  "- 8 a 10: Alto risco (requer validação adicional urgente - BV)"
  "Exemplo: 'Risco de Lavagem de Dinheiro: 6/10'"
)




def get_chatgpt_response(prompt, model="gpt-4o-2024-11-20"):
  """
  Envia um prompt para o modelo GPT especificado e retorna a resposta.
   Args:
      prompt (str): O prompt ou contexto a ser analisado.
      model (str): O modelo GPT a ser utilizado (padrão: "gpt-4o-2024-11-20").
   Returns:
      str: Resposta do modelo ou uma mensagem de erro customizada.
  """
  try:
      # Configura os parâmetros básicos
      params = {
          "model": model,
          "messages": [
              {"role": "system", "content": SYSTEM_PROMPT},
              {"role": "user", "content": prompt},
          ]
      }
      # Define parâmetros específicos conforme o modelo
      if model == "gpt-4o-2024-11-20":
          params["temperature"] = 0.0
      elif model == "o3-mini-2025-01-31":
          params["reasoning_effort"] = "high"
        
      response = client.chat.completions.create(**params)
      return response.choices[0].message.content.strip()
  except Exception as e:
      error_message = str(e)
      if 'context_length_exceeded' in error_message.lower():
          return "Opa! Não consigo tankar este caso, pois há muitas transações. Chame um analista humano - ou reptiliano - para resolver"
      else:
          return f"An error occurred: {error_message}"




def get_analysis_and_decision(prompt):
  """
  Realiza a análise completa do caso utilizando o GPT‑4 e, a partir dessa análise,
  solicita ao modelo o3-mini-2025-01-31 que gere, em exatamente duas linhas, a decisão final.
  Todo o resultado é retornado em uma única string para manter o mesmo retorno do prompt original.
   Args:
      prompt (str): Os dados do caso para análise.
   Returns:
      str: Uma string contendo a análise completa seguida da decisão final.
  """
  # Realiza a análise completa com o GPT‑4
  analysis = get_chatgpt_response(prompt, model="gpt-4o-2024-11-20")
   # Verifica se o score de risco já está presente na análise
  if "Risco de Lavagem de Dinheiro:" not in analysis:
      # Se não estiver, solicita ao GPT para adicionar um score de risco
      score_prompt = (
          "Com base na análise a seguir, classifique o risco de lavagem de dinheiro em uma escala de 1 a 10, "
          "onde 1-3 é baixo risco, 4-7 é médio risco, e 8-10 é alto risco.\n\n"
          f"{analysis}\n\n"
          "Responda apenas com: Risco de Lavagem de Dinheiro: [número]/10"
      )
      score_response = get_chatgpt_response(score_prompt, model="o3-mini-2025-01-31")
    
      # Adiciona o score à análise
      analysis += f"\n\n{score_response}"
   # Cria o prompt para o o3-mini, solicitando a decisão final em exatamente duas linhas
  decision_prompt = (
      "A partir da análise detalhada a seguir, por favor, em exatamente duas linhas, apresente a decisão final sobre o caso. "
      "Inclua o score de risco na sua decisão. "
      "Caso haja necessidade de solicitar documentos (comprovante de endereço e de renda), inclua o pedido de forma concisa:\n\n"
      f"{analysis}"
  )
  decision = get_chatgpt_response(decision_prompt, model="o3-mini-2025-01-31")
   # Junta a análise e a decisão final em uma única string
  result = analysis + "\n\nDecisão Final do 03mini:\n" + decision
  return result




# Exemplo de uso:
# case_data = "Dados do caso: [insira os dados do caso aqui]"
# resultado = get_analysis_and_decision(case_data)
# print(resultado)












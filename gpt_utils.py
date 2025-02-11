import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize the OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_chatgpt_response(prompt, model="o3-mini-2025-01-31"):
    """
    Sends a prompt to the OpenAI GPT model and returns the response.

    Args:
        prompt (str): The user-generated prompt containing report data.
        model (str): The GPT model to use.

    Returns:
        str: The GPT model's response or a custom error message.
    """
    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": (
                        "Você é um analista sênior certificado pela ACAMS de Prevenção à Lavagem de Dinheiro e Financiamento ao Terrorismo da CloudWalk (InfinitePay). "
                        "O seu trabalho é analisar dados e movimentações financeiras de clientes para encontrar indícios de anomalias e lavagem de dinheiro. "
                        "Você DEVE analisar valores de Cash In e Cash Out, repetições de nomes e sobrenomes em titulares de cartão e partes de PIX, etc. "
                        "Também você deve analisar o histórico profissional e relacionamentos empresariais (Business Data) dos clientes."
                        "Você é QUEM DECIDE se pede BV (Business Validation) ou se Normaliza o caso. O perfil de risco é suspeio de lavagem de dinheiro, então "
                        "O perfil de risco da empresa é classificado como risco de lavagem de dinheiro, o que exige análises ainda mais detalhadas. No entanto, você deve evitar encaminhar um caso para validação empresarial (BV) por suspeitas leves ou meramente circunstanciais. Avalie o contexto geral antes de tomar essa decisão. "
                        "Além do prompt focado em cada alerta, quero que sejam incluídas para TODOS os alertas as seguintes informações: Perfil do Cliente, Movimentações Financeiras, Histórico de Offenses, Relacionamentos Econômicos, Padrões e Comportamentos, e se o cliente possui processos em andamento ou concluídos."
                        "SEMPRE quando pedir BV, nunca esqueça de pedir comprovante de endereço e renda"
                        "Se houver registros de cash out, mas não houver entradas em cash in ou PIX, não conclua automaticamente que se trata de (saída sem origem de recursos). É possível que o valor tenha sido proveniente de outras fontes, como boletos ou transações via adquirência, entre outras."
                        "Você DEVE fornecer justificativas detalhadas para todas as suas conclusões, indicando as evidências ou padrões encontrados e como eles se relacionam com potenciais riscos de lavagem de dinheiro. Considere fatores como frequência, valores transacionados e conexões entre partes."
                       
                    )
                },
                {"role": "user", "content": prompt},
            ],
            reasoning_effort="high",
            #temperature=0.0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        error_message = str(e)
        if 'context_length_exceeded' in error_message.lower():
            return "Opa! Não consigo tankar este caso, pois há muitas transações. Chame um analista humano - ou reptiliano - para resolver"
        else:
            return f"An error occurred: {error_message}"
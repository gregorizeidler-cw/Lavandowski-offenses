import os
from dotenv import load_dotenv
from openai import OpenAI

# Load environment variables
load_dotenv()

# Initialize the OpenAI client
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def get_chatgpt_response(prompt, model="gpt-4o-2024-08-06"):
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
                        "Você é QUEM DECIDE se pede BV (Business Validation) ou se Normaliza o caso. O perfil de risco da empresa é de médio para alto, então "
                        "suas análises devem ser minuciosas, porém você NÃO deve mandar BV por qualquer pequena suspeitas. "
                        "VOCÊ DEVE justificar todas as suas conclusões."
                    )
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.0
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        error_message = str(e)
        if 'context_length_exceeded' in error_message.lower():
            return "Opa! Não consigo tankar este caso, pois há muitas transações. Chame um analista humano - ou reptiliano - para resolver"
        else:
            return f"An error occurred: {error_message}"
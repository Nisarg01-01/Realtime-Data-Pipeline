import os
from sqlalchemy import create_engine
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_community.utilities import SQLDatabase
from langchain.agents import create_sql_agent

os.environ["GOOGLE_API_KEY"] = "AIzaSyBXoJDUz_msPYgbHOgMIVUtvaG2aq7HyVk"

def create_db_agent():
    db_url = "postgresql://user:password@localhost:5432/ecommerce"
    db_engine = create_engine(db_url)

    db = SQLDatabase(engine=db_engine)

    llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash-latest", temperature=0)

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    agent_executor = create_sql_agent(
        llm=llm,
        toolkit=toolkit,
        verbose=True,
        agent_type="openai-tools",
    )
    return agent_executor

def ask_agent(agent, question):
    try:
        response = agent.invoke({"input": question})
        return response.get("output", "Sorry, I couldn't find an answer to that question.")
    except Exception as e:
        return f"An error occurred: {str(e)}"
    
if __name__ == "__main__":
    print("Testing the SQL Agent...")
    agent = create_db_agent()
    question = "What were the top 3 most purchased brands?"
    answer = ask_agent(agent, question)
    print(f"Question: {question}\nAnswer: {answer}")
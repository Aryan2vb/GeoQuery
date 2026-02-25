from abc import ABC, abstractmethod


class LLMProvider(ABC):
    """Abstract base class for LLM providers."""

    @abstractmethod
    def generate(self, prompt: str) -> str:
        """Generate a response from the LLM given a prompt."""
        pass

    @abstractmethod
    def extract_sql(self, response: str) -> str:
        """Extract SQL query from LLM response."""
        pass


class OpenAIProvider(LLMProvider):
    """OpenAI implementation of LLMProvider."""

    def __init__(self, api_key: str, model: str = "gpt-4o-mini"):
        self.api_key = api_key
        self.model = model

    def generate(self, prompt: str) -> str:
        from openai import OpenAI

        client = OpenAI(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a PostGIS expert."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        return response.choices[0].message.content

    def extract_sql(self, response: str) -> str:
        import re

        response = response.strip()

        sql_block = re.search(r"```sql\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_block = re.search(r"```\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_match = re.search(r"(SELECT\s+.+?;)", response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()

        return response.strip()


class LangChainProvider(LLMProvider):
    """LangChain implementation of LLMProvider."""

    def __init__(self, llm, output_parser=None):
        self.llm = llm
        self.output_parser = output_parser

    def generate(self, prompt: str) -> str:
        if self.output_parser:
            return self.output_parser.parse(self.llm.invoke(prompt))
        return self.llm.invoke(prompt)

    def extract_sql(self, response: str) -> str:
        import re

        sql_block = re.search(r"```sql\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_block = re.search(r"```\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_match = re.search(r"(SELECT\s+.+?;)", response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()

        return response.strip()


class GroqProvider(LLMProvider):
    """Groq implementation of LLMProvider (OpenAI-compatible API)."""

    def __init__(self, api_key: str, model: str = "openai/gpt-oss-safeguard-20b"):
        self.api_key = api_key
        self.model = model

    def generate(self, prompt: str) -> str:
        from groq import Groq

        client = Groq(api_key=self.api_key)
        response = client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": "You are a PostGIS expert."},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )
        return response.choices[0].message.content

    def extract_sql(self, response: str) -> str:
        import re

        sql_block = re.search(r"```sql\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_block = re.search(r"```\n(.+?)```", response, re.DOTALL)
        if sql_block:
            return sql_block.group(1).strip()

        sql_match = re.search(r"(SELECT\s+.+?;)", response, re.DOTALL | re.IGNORECASE)
        if sql_match:
            return sql_match.group(1).strip()

        return response.strip()

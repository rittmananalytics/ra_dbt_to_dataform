import re
import json
import openai
from pathlib import Path
from dbt_to_dataform.conversion_report import ConversionReport

class SyntaxChecker:
    def __init__(self, openai_api_key: str):
        self.openai_api_key = openai_api_key
        openai.api_key = openai_api_key

    def check_and_correct_syntax(self, file_path: Path, content: str, conversion_report: ConversionReport) -> tuple:
        print(f"Checking syntax for file: {file_path}")
        
        if not self.openai_api_key:
            print("OpenAI API key not provided. Skipping syntax check.")
            return content, None

        if not isinstance(content, str):
            print(f"Warning: content is not a string. Type: {type(content)}")
            return str(content) if content is not None else "", None

        file_type = self._get_file_type(file_path)
        prompt = self._generate_prompt(file_type, content)

        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You are an expert in Dataform syntax and SQL. Your task is to check and correct Dataform SQLX and JSON configuration files."},
                    {"role": "user", "content": prompt}
                ]
            )

            result = response.choices[0].message.content.strip()
            print(f"OpenAI response received for {file_path}")

            if result.lower() != "valid":
                conversion_report.add_issue(
                    str(file_path),
                    "Syntax Correction",
                    f"The following changes were made: {result}"
                )
                print(f"Syntax corrections made for {file_path}")
                corrected_code = self._extract_corrected_code(result, file_type)
                return corrected_code if corrected_code else content, result
            else:
                print(f"No syntax corrections needed for {file_path}")
            
            return content, None

        except Exception as e:
            print(f"Error during syntax check for {file_path}: {str(e)}")
            return content, None

    def _get_file_type(self, file_path: Path) -> str:
        if file_path.suffix == '.sqlx':
            return 'sqlx'
        elif file_path.name == 'dataform.json':
            return 'json'
        else:
            return 'unknown'

    def _generate_prompt(self, file_type: str, content: str) -> str:
        if file_type == 'sqlx':
            return f"""
            Check if the following Dataform SQLX code is valid. If it's not valid, correct it and explain the changes made.
            If it's valid, just respond with "Valid".

            Always include the full corrected code in your response, even if only small changes were made.
            Wrap the corrected code in ```sqlx and ``` tags.

            Code:
            {content}
            """
        elif file_type == 'json':
            return f"""
            Check if the following dataform.json configuration is valid. If it's not valid, correct it and explain the changes made.
            If it's valid, just respond with "Valid".

            Always include the full corrected JSON in your response, even if only small changes were made.
            Wrap the corrected JSON in ```json and ``` tags.

            JSON:
            {content}
            """
        else:
            return f"Unknown file type. Please check the content:\n\n{content}"

    def _extract_corrected_code(self, result: str, file_type: str) -> str:
        if file_type == 'sqlx':
            sqlx_code_blocks = re.findall(r'```sqlx(.*?)```', result, re.DOTALL)
            if sqlx_code_blocks:
                return sqlx_code_blocks[-1].strip()
        elif file_type == 'json':
            json_code_blocks = re.findall(r'```json(.*?)```', result, re.DOTALL)
            if json_code_blocks:
                try:
                    # Attempt to parse the JSON to ensure it's valid
                    json.loads(json_code_blocks[-1].strip())
                    return json_code_blocks[-1].strip()
                except json.JSONDecodeError:
                    print("Warning: Extracted JSON is not valid.")

        # If no specific code blocks found, fall back to general extraction
        code_blocks = re.findall(r'```(.*?)```', result, re.DOTALL)
        if code_blocks:
            return code_blocks[-1].strip()

        corrected_code_match = re.search(r'(?:Corrected|Fixed|Updated) (?:code|version):\s*(.*)', result, re.DOTALL | re.IGNORECASE)
        if corrected_code_match:
            return corrected_code_match.group(1).strip()

        lines = result.split('\n')
        for i, line in enumerate(lines):
            if file_type == 'sqlx' and (line.strip().endswith('{') or line.strip().startswith('config')):
                return '\n'.join(lines[i:]).strip()
            elif file_type == 'json' and line.strip().startswith('{'):
                return '\n'.join(lines[i:]).strip()

        return ""
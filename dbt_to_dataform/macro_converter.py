# macro_converter.py

from pathlib import Path
from langchain.chat_models import ChatOpenAI
from langchain.prompts import ChatPromptTemplate
from langchain.chains import LLMChain

class MacroConverter:
    def __init__(self, openai_api_key):
        self.llm = ChatOpenAI(temperature=0.2, model_name="gpt-3.5-turbo", openai_api_key=openai_api_key)
        self.macro_conversion_prompt = ChatPromptTemplate.from_template("""
            Convert the following dbt macro to a JavaScript function for Dataform:

            {macro_content}

            Follow these guidelines:
            1. Convert Python/Jinja syntax to JavaScript.
            2. Replace dbt-specific functions with Dataform equivalents where possible.
            3. For SQL generation, use JavaScript template literals.
            4. If there's no direct Dataform equivalent for a dbt function, implement the functionality in JavaScript.
            5. Add any comments or explanations either inline or underneath the code, otherwise just output the converted JavaScript function.
            6. Do not wrap the function in any additional code or comments.

            Provide only the converted JavaScript function:
            """)
        self.macro_conversion_chain = LLMChain(llm=self.llm, prompt=self.macro_conversion_prompt)

    def convert_macros(self, dbt_project_path: Path, dataform_output_path: Path):
        macros_dir = Path(dbt_project_path) / 'macros'
        dataform_includes_dir = Path(dataform_output_path) / 'includes'
        dataform_includes_dir.mkdir(parents=True, exist_ok=True)

        for macro_file in macros_dir.glob('*.sql'):
            with open(macro_file, 'r') as f:
                macro_content = f.read()

            converted_js = self.macro_conversion_chain.run(macro_content=macro_content)

            output_file = dataform_includes_dir / f"{macro_file.stem}.js"
            with open(output_file, 'w') as f:
                f.write(converted_js.strip())  # Remove any leading/trailing whitespace

            print(f"Converted {macro_file.name} to {output_file.name}")

    def update_macro_references(self, dataform_output_path: Path):
        definitions_dir = Path(dataform_output_path) / 'definitions'
        for js_file in definitions_dir.rglob('*.js'):
            with open(js_file, 'r') as f:
                content = f.read()

            # Update macro references
            # This is a simplified example and might need to be adjusted based on your specific macro usage
            content = content.replace('{{ ', '${')
            content = content.replace(' }}', '}')

            with open(js_file, 'w') as f:
                f.write(content)

            print(f"Updated macro references in {js_file.name}")
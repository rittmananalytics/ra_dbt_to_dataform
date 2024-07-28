import os
import re
import argparse
from pathlib import Path
import yaml
import traceback
import sys

from dbt_to_dataform.repository_analyzer import RepositoryAnalyzer
from dbt_to_dataform.model_converter import ModelConverter
from dbt_to_dataform.metadata_converter import MetadataConverter
from dbt_to_dataform.project_generator import ProjectGenerator
from dbt_to_dataform.project_config_converter import ProjectConfigConverter
from dbt_to_dataform.macro_converter import MacroConverter
from dbt_to_dataform.source_converter import SourceConverter
from dbt_to_dataform.conversion_report import ConversionReport
from dbt_to_dataform.syntax_checker import SyntaxChecker

def main(dbt_repo_path: str, output_path: str, openai_api_key: str = None, verbose: bool = False):

    # Initialize components
    analyzer = RepositoryAnalyzer(dbt_repo_path)
    project_generator = ProjectGenerator(output_path)

    print("Analyzing dbt repository...")
    artifacts = analyzer.analyze()
    dbt_config = analyzer.get_project_config()
    conversion_report = ConversionReport(Path(output_path))
    syntax_checker = SyntaxChecker(openai_api_key) if openai_api_key else None

    # Extract project variables
    project_variables = dbt_config.get('vars', {})

    # Initialize converters with project variables
    dbt_models_dir = Path(dbt_repo_path) / 'models'

    print("Converting dbt project configuration...")
    dbt_project_path = Path(dbt_repo_path) / 'dbt_project.yml'
    dataform_config_path = Path(output_path) / 'dataform.json'
    project_config_converter = ProjectConfigConverter(dbt_project_path, dataform_config_path)
    project_config_converter.convert()

    print("Generating Dataform project structure...")
    project_generator.generate_project_structure()

     
    print("Converting sources...")
    source_converter = SourceConverter(Path(dbt_repo_path), Path(output_path))
    source_tables = source_converter.convert_sources()
    
    if openai_api_key:
        print("Converting macros...")
        macro_converter = MacroConverter(openai_api_key)
        macro_converter.convert_macros(dbt_repo_path, output_path)

    print("Converting models...")
    model_converter = ModelConverter(project_variables, dbt_models_dir, source_tables)

    for model_path in artifacts['models']:
        try:
            sqlx_content, output_dir, output_file = model_converter.convert_model(model_path)
            if sqlx_content is None or output_dir is None or output_file is None:
                print(f"Skipping model due to conversion error: {model_path}")
                continue

            output_model_path = Path(output_path) / 'definitions' / output_dir
            output_model_path.mkdir(parents=True, exist_ok=True)
            output_file_path = output_model_path / output_file

            
            # Adjust source references
            #sqlx_content = re.sub(r'\$\{ref\([\'"](\w+)[\'"]\)\}', lambda m: f"${{ref('source_{m.group(1)}')}}", sqlx_content)

            print(f"Converting model: {model_path.relative_to(dbt_models_dir)} to {output_file_path}")

            # Check and correct syntax if OpenAI API key is provided
            if syntax_checker:
                print(f"Performing syntax check for {output_file_path}")
                sqlx_content, corrections = syntax_checker.check_and_correct_syntax(output_file_path, sqlx_content, conversion_report)
                if verbose and corrections:
                    print(f"Syntax corrections for {output_file_path}:")
                    print(corrections)
            else:
                print("Syntax checker not available. Skipping syntax check.")

            if not isinstance(sqlx_content, str):
                print(f"Warning: sqlx_content is not a string. Type: {type(sqlx_content)}")
                sqlx_content = str(sqlx_content) if sqlx_content is not None else ""

            print(f"Writing content to {output_file_path}")
            output_file_path.write_text(sqlx_content)

            # Check for potential issues
            if "-- TODO:" in sqlx_content:
                conversion_report.add_issue(
                    str(model_path),
                    "Incomplete Conversion",
                    "This model contains TODO comments indicating manual review is needed."
                )
            if "dbt_utils" in sqlx_content:
                conversion_report.add_issue(
                    str(model_path),
                    "Unconverted dbt_utils Reference",
                    "This model still contains references to dbt_utils that couldn't be automatically converted."
                )
        except Exception as e:
            print(f"Error converting model: {model_path.relative_to(dbt_models_dir)}")
            print(f"Error message: {str(e)}")
            print("Traceback:")
            traceback.print_exc()
            print("Skipping this model and continuing with the next...")
            conversion_report.add_issue(
                str(model_path),
                "Conversion Error",
                f"Error occurred during conversion: {str(e)}"
            )

    print("Converting metadata...")
    for yaml_path in artifacts['yaml_files']:
        if yaml_path.name == 'schema.yml':
            try:
                relative_path = yaml_path.relative_to(analyzer.dbt_project_path)
                output_def_path = Path(output_path) / 'definitions' / relative_path.with_suffix('.sqlx')
                output_def_path.parent.mkdir(parents=True, exist_ok=True)

                print(f"Converting metadata: {relative_path}")
                dataform_sqlx = metadata_converter.convert_schema_yml(yaml_path)
                if dataform_sqlx:
                    if syntax_checker:
                        print(f"Performing syntax check for metadata: {output_def_path}")
                        dataform_sqlx, corrections = syntax_checker.check_and_correct_syntax(output_def_path, dataform_sqlx, conversion_report)
                        if verbose and corrections:
                            print(f"Syntax corrections for {output_def_path}:")
                            print(corrections)
                    output_def_path.write_text(dataform_sqlx)
                else:
                    print(f"Skipping empty or invalid schema file: {yaml_path}")
            except Exception as e:
                print(f"Error converting metadata: {relative_path}")
                print(f"Error message: {str(e)}")
                print("Traceback:")
                traceback.print_exc()
                print("Skipping this metadata file and continuing with the next...")

    if openai_api_key:
        print("Updating macro references...")
        macro_converter.update_macro_references(output_path)

    conversion_report.generate_report()

    print("Conversion complete!")
    
def cli():
    parser = argparse.ArgumentParser(description="Convert dbt project to Dataform")
    parser.add_argument("dbt_repo_path", help="Path to the local dbt repository")
    parser.add_argument("output_path", help="Path to output the Dataform project")
    parser.add_argument("--openai-api-key", help="OpenAI API key for complex conversions", default=None)
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output")

    args = parser.parse_args()

    main(args.dbt_repo_path, args.output_path, args.openai_api_key, args.verbose)

if __name__ == "__main__":
    cli()
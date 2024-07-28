# dbt to Dataform Converter

This tool automates the process of converting dbt (data build tool) projects to Dataform, focusing on BigQuery as the data warehouse. It leverages OpenAI's API for complex conversions and syntax checking.

# Features

- Converts dbt models to Dataform SQLX files
- Translates dbt source definitions to Dataform declarations
- Converts dbt macros to JavaScript functions
- Migrates dbt tests to Dataform assertions
- Preserves project structure, adapting it to Dataform best practices
- Handles dbt-specific Jinja syntax and converts it to JavaScript
- Supports conversion of dbt variables to Dataform project config variables
- Automatically converts common dbt_utils functions to their BigQuery equivalents
- Uses GPT-4 to check and correct Dataform syntax in converted files (requires OpenAI API key)
- Generates a detailed conversion report highlighting potential issues and syntax corrections

## Components

1. **RepositoryAnalyzer** (`repository_analyzer.py`)
   - Analyzes the structure of the dbt project
   - Identifies models, tests, macros, and YAML files

2. **ModelConverter** (`model_converter.py`)
   - Converts dbt SQL models to Dataform SQLX files
   - Handles reference conversions, variable replacements, and macro translations

3. **SourceConverter** (`source_converter.py`)
   - Converts dbt source definitions to Dataform source declarations
   - Creates individual SQLX files for each source table

4. **MacroConverter** (`macro_converter.py`)
   - Converts dbt macros to Dataform JavaScript functions
   - Uses OpenAI API for complex macro conversions

5. **ProjectConfigConverter** (`project_config_converter.py`)
   - Converts dbt project configuration to Dataform config
   - Handles variable translations and project-wide settings

6. **ProjectGenerator** (`project_generator.py`)
   - Generates the Dataform project structure

7. **SyntaxChecker** (`syntax_checker.py`)
   - Uses OpenAI API to check and correct Dataform syntax in converted files

8. **ConversionReport** (`conversion_report.py`)
   - Generates a detailed report of the conversion process
   - Highlights potential issues and syntax corrections

9. **Main Script** (`main.py`)
   - Orchestrates the entire conversion process
   - Handles command-line arguments and initializes components

## OpenAI API Usage

This tool utilizes the OpenAI API in two main areas:

1. **Complex Macro Conversions**: 
   - The `MacroConverter` class uses the OpenAI API to convert complex dbt macros to Dataform JavaScript functions.
   - It sends the dbt macro code to the API and receives a converted JavaScript function.

2. **Syntax Checking and Correction**:
   - The `SyntaxChecker` class uses the OpenAI API to verify and correct the syntax of converted Dataform files.
   - It sends the converted SQLX content to the API, which checks for Dataform-specific syntax issues and suggests corrections.

The OpenAI API key is optional but recommended for better conversion results, especially for complex macros and ensuring Dataform-compliant syntax.

## How does it work

The migration tool employs a combination of rule-based transformations for standard conversions and AI-powered processing for more complex scenarios. This hybrid approach enables the tool to handle both straightforward translations and nuanced, context-dependent conversions effectively.

While the process is largely automated, it is designed to complement rather than replace human expertise. The tool provides a solid foundation for migration, but user intervention may be necessary for project-specific optimizations and handling of unsupported features.

The migration process comprises seven steps:

1. **Project Analysis**: 
   - The RepositoryAnalyzer scans the dbt project structure.
   - It identifies models, tests, macros, and YAML files.

2. **Project Configuration Conversion**:
   - The ProjectConfigConverter translates dbt_project.yml to dataform.json.
   - It handles project-wide settings and variables.

3. **Source Conversion**:
   - The SourceConverter processes dbt source definitions.
   - It creates individual SQLX files for each source table in the Dataform project.

4. **Model Conversion**:
   - The ModelConverter translates each dbt SQL model to a Dataform SQLX file.
   - It handles reference conversions, variable replacements, and macro translations.

5. **Macro Conversion**:
   - The MacroConverter transforms dbt macros into Dataform JavaScript functions.
   - Complex macros are converted using the OpenAI API.

6. **Syntax Checking and Correction**:
   - The SyntaxChecker uses the OpenAI API to verify and correct Dataform syntax in converted files.

7. **Report Generation**:
   - The ConversionReport creates a detailed report of the conversion process.
   - It highlights potential issues, syntax corrections, and areas needing manual review.

## Automatically Converted dbt_utils Functions

The following dbt_utils functions are automatically converted to their BigQuery equivalents:

1. `{{ dbt_utils.type_string() }}` -> `STRING`
2. `{{ dbt_utils.type_int() }}` -> `INT64`
3. `{{ dbt_utils.type_numeric() }}` -> `NUMERIC`
4. `{{ dbt_utils.type_timestamp() }}` -> `TIMESTAMP`
5. `{{ dbt_utils.star(from=ref('model_name')) }}` -> `*`
6. `{{ dbt_utils.surrogate_key(['col1','col2']) }}` -> `TO_HEX(MD5(CONCAT(CAST(col1 AS STRING), CAST(col2 AS STRING))))`
7. `{{ dbt_utils.datediff(...) }}` -> `DATE_DIFF(...)`
8. `{{ dbt_utils.dateadd(...) }}` -> `DATE_ADD(...)`
9. `{{ dbt_utils.date_trunc(...) }}` -> `DATE_TRUNC(...)`
10. `{{ dbt_utils.date_part(...) }}` -> `EXTRACT(...)`

## Use of OpenAI API

1. **Complex Macro Conversions**:
   - The MacroConverter sends complex dbt macros to the OpenAI API for conversion.
   - Prompt example:
     ```
     Convert the following dbt macro to a JavaScript function for Dataform:

     {dbt_macro_content}

     Follow these guidelines:
     1. Convert Python/Jinja syntax to JavaScript.
     2. Replace dbt-specific functions with Dataform equivalents where possible.
     3. For SQL generation, use JavaScript template literals.
     4. If there's no direct Dataform equivalent for a dbt function, implement the functionality in JavaScript.
     5. Add any necessary comments or explanations.

     Provide only the converted JavaScript function:
     ```
   - The API returns a JavaScript function that can be used in Dataform.

2. **Syntax Checking and Correction**:
   - The SyntaxChecker sends converted SQLX content to the OpenAI API for verification and correction.
   - Prompt example:
     ```
     Check if the following Dataform SQLX code is valid. If it's not valid, correct it and explain the changes made.
     If it's valid, just respond with "Valid".

     Always include the full corrected code in your response, even if only small changes were made.
     Wrap the corrected code in ```sqlx and ``` tags.

     Code:
     {sqlx_content}
     ```
   - The API returns either "Valid" or a corrected version of the SQLX code with explanations.

## Unsupported dbt Features

While this converter handles many aspects of dbt projects, some features are not currently supported or require manual intervention:

1. **Seeds**: The converter does not automatically handle dbt seed files. These CSV files need to be manually imported into your data warehouse and declared in Dataform.

2. **dbt Semantic Layer**: Dataform does not have an equivalent to dbt's semantic layer. Metric definitions and semantic models will need to be reimplemented using Dataform's capabilities.

3. **Snapshots**: While the converter attempts to translate dbt snapshots, Dataform's approach to slowly changing dimensions (SCDs) differs from dbt's. Manual adjustment may be necessary.

4. **Custom Tests**: dbt's custom tests don't have a direct equivalent in Dataform. These will need to be reimplemented using Dataform's assertion capabilities.

5. **Packages**: dbt packages are not automatically converted. You'll need to find Dataform equivalents or reimplement the functionality.

6. **Documentation**: dbt's documentation generation is not directly translated. Dataform has its own documentation features that will need to be set up manually.

7. **Exposures**: Dataform doesn't have a direct equivalent to dbt's exposures. This information will need to be managed outside of Dataform.

8. **Advanced Hooks**: While basic pre- and post-hooks can be converted, advanced hook usage in dbt might require manual implementation in Dataform.

Always review the conversion report and test thoroughly after conversion to ensure all critical functionality is preserved.

# Installation

1. Clone this repository:
```
git clone https://github.com/your-username/dbt-to-dataform.git
```
2. Navigate to the project root directory:
```
cd dbt-to-dataform
```
3. Install the package:
```
pip install -e .
```
## Usage

After installation, you can use the tool from the command line:

```bash
dbt-to-dataform <dbt_repo_path> <output_path> --openai-api-key <your-api-key> --verbose

<dbt_repo_path>: Path to the local dbt repository
<output_path>: Path to output the Dataform project
--openai-api-key: Optional. Your OpenAI API key for complex conversions and syntax checking
--verbose: Optional. Enable verbose output
Post-Conversion Steps
After running the converter:

Review the conversion_report.json and conversion_summary.txt files
Address any issues highlighted in the conversion report
Review and test all converted models, especially those flagged in the report
Implement any custom logic that couldn't be automatically converted
Update any remaining dbt-specific syntax or functions that weren't automatically handled

Limitations

Complex dbt macros may require manual adjustment after conversion
Custom dbt tests might need additional implementation in Dataform
The tool assumes a BigQuery setup; adjustments may be needed for other warehouses
Certain dbt-specific features might not have direct equivalents in Dataform

Contributing
Contributions to improve the converter are welcome. Please submit pull requests with clear descriptions of the changes and their purposes.
License
MIT License
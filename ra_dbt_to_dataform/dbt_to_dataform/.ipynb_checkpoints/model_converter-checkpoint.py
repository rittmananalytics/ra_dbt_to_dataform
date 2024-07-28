import re
from pathlib import Path
import yaml

class ModelConverter:
    def __init__(self, project_variables: dict, dbt_models_dir: Path, source_tables: set):
        self.project_variables = project_variables
        self.dbt_models_dir = dbt_models_dir
        self.source_tables = source_tables

    def convert_model(self, dbt_model_path: Path) -> tuple:
            try:
                with open(dbt_model_path, 'r') as f:
                    dbt_content = f.read()

                # Convert config block
                config_block = self._convert_config(dbt_content)

                # Convert SQL content
                sql_content = self._convert_sql(dbt_content)

                if sql_content is None:
                    raise ValueError("SQL content conversion failed")

                sqlx_content = f"{config_block}\n\n{sql_content}"

                # Determine the appropriate output directory
                if 'marts' in str(dbt_model_path) or self._is_final_model(dbt_content):
                    base_output_dir = 'output'
                else:
                    base_output_dir = 'intermediate'

                # Get the relative path of the model within the dbt models directory
                relative_path = dbt_model_path.relative_to(self.dbt_models_dir)

                # Combine the base output directory with the relative path
                output_dir = Path(base_output_dir) / relative_path.parent

                # Create the output file name with .sqlx extension
                output_file = relative_path.with_suffix('.sqlx').name

                return sqlx_content, output_dir, output_file

            except Exception as e:
                print(f"Error in convert_model for {dbt_model_path}: {str(e)}")
                print("Traceback:")
                import traceback
                traceback.print_exc()
                return None, None, None


    def _is_final_model(self, content: str) -> bool:
        # Implement logic to determine if a model is a final output
        # This is a placeholder implementation
        return False

    def _convert_config(self, content: str) -> str:
        config_match = re.search(r'\{\{\s*config\((.*?)\)\s*\}\}', content, re.DOTALL)
        if config_match:
            config_content = config_match.group(1)
            config_dict = yaml.safe_load(f"config: {{{config_content}}}")['config']
            config_items = []
            
            # Set default type if not specified
            if 'materialized' not in config_dict:
                config_items.append("  type: \"table\"")
            
            for k, v in config_dict.items():
                if k == 'materialized':
                    config_items.append(f"  type: \"{v}\"")
                elif k == 'enabled':
                    if isinstance(v, str) and v.startswith('var('):
                        var_name = re.search(r'var\([\'"](\w+)[\'"]\)', v).group(1)
                        config_items.append(f"  disabled: ${{!dataform.projectConfig.vars.{var_name}}}")
                    else:
                        config_items.append(f"  disabled: {str(not v).lower()}")
                else:
                    config_items.append(f"  {k}: {self._format_config_value(v)}")
            
            return "config {\n" + ",\n".join(config_items) + "\n}"
        return "config {\n  type: \"table\"\n}"

    def _format_config_value(self, value):
        if isinstance(value, str):
            return f"\"{value}\""
        elif isinstance(value, bool):
            return str(value).lower()
        else:
            return str(value)

    def _convert_sql(self, content: str) -> str:
        # Remove config block
        sql_content = re.sub(r'\{\{\s*config\(.*?\)\s*\}\}', '', content, flags=re.DOTALL)
        
        # Convert set blocks
        sql_content = self._convert_set_blocks(sql_content)
        
        # Convert references
        sql_content = self._convert_references(sql_content)
        
        # Convert variables
        sql_content = self._convert_variables(sql_content)
        
        # Convert conditionals
        sql_content = self._convert_conditionals(sql_content)
        
        # Convert for loops
        sql_content = self._convert_for_loops(sql_content)
        
        # Convert macros
        sql_content = self._convert_macros(sql_content)
        
        # Convert comments
        sql_content = self._convert_comments(sql_content)
        
        # Convert is_incremental() to incremental()
        sql_content = self._convert_incremental(sql_content)
        
        return sql_content.strip()

    def _convert_set_blocks(self, content: str) -> str:
        def replace_set(match):
            var_name, var_content = match.groups()
            return f"let {var_name} = sql.identifier(`{var_content.strip()}`);"
        
        return re.sub(
            r'{%\s*set\s+(\w+)\s*%}(.*?){%\s*endset\s*%}',
            replace_set,
            content,
            flags=re.DOTALL
        )

    def _convert_references(self, content: str) -> str:
            # Convert dbt refs
            content = re.sub(r'\{\{\s*ref\([\'"](\w+)[\'"]\)\s*\}\}', lambda m: self._ref_replacement(m.group(1)), content)

            # Convert dbt sources
            content = re.sub(r'\{\{\s*source\([\'"](\w+)[\'"]\s*,\s*[\'"](\w+)[\'"]\)\s*\}\}', lambda m: self._source_replacement(m.group(2)), content)

            # Convert 'this' references
            content = re.sub(r'\{\{\s*this\s*\}\}', r'${self()}', content)

            return content

    def _ref_replacement(self, name):
        if name in self.source_tables:
            return f"${{ref('source_{name}')}}"
        else:
            return f"${{ref('{name}')}}"

    def _source_replacement(self, name):
        return f"${{ref('source_{name}')}}"

    def _convert_variables(self, content: str) -> str:
        # Convert var() references
        content = re.sub(
            r'\{\{\s*var\([\'"](\w+)[\'"]\)\s*\}\}',
            r'${dataform.projectConfig.vars.\1}',
            content
        )
        
        # Convert not var() references
        content = re.sub(
            r'not\s+var\([\'"](\w+)[\'"]\)',
            r'!dataform.projectConfig.vars.\1',
            content
        )
        
        return content

    def _convert_conditionals(self, content: str) -> str:
        content = re.sub(r'{%\s*if\s+(.*?)\s*%}', r'${ when(\1, `', content)
        content = re.sub(r'{%\s*elif\s+(.*?)\s*%}', r'`) } ${ when(\1, `', content)
        content = re.sub(r'{%\s*else\s*%}', r'`) } ${ otherwise(`', content)
        content = re.sub(r'{%\s*endif\s*%}', r'`) }', content)
        return content

    def _convert_for_loops(self, content: str) -> str:
        def convert_for_loop(match):
            loop_var = match.group(1)
            iterable = match.group(2)
            loop_content = match.group(3)
            return f"${{{{ {iterable}.map({loop_var} => `\n{loop_content}\n`).join('') }}}}"

        return re.sub(r'{%\s*for\s+(\w+)\s+in\s+(.*?)\s*%}(.*?){%\s*endfor\s*%}', convert_for_loop, content, flags=re.DOTALL)

    def _convert_dbt_utils_type_functions(self, content: str) -> str:
        type_conversions = {
            'type_string': 'STRING',
            'type_int': 'INT64',
            'type_numeric': 'NUMERIC',
            'type_timestamp': 'TIMESTAMP'
        }
        for dbt_type, bigquery_type in type_conversions.items():
            pattern = r'\{{\s*dbt_utils\.' + re.escape(dbt_type) + r'\(\)\s*\}}'
            content = re.sub(pattern, bigquery_type, content)
        return content

    def _convert_dbt_utils_star(self, content: str) -> str:
        pattern = r'{{\s*(?:dbt_utils|dbt)\.star\((from=)?ref\([\'"](\w+)[\'"]\)(?:,\s*except=\[[^\]]*\])?\)\s*}}'
        return re.sub(pattern, '*', content)

    def _convert_dbt_utils_surrogate_key(self, content: str) -> str:
        def replace_surrogate_key(match):
            columns = match.group(1).strip('[]').replace("'", "").replace('"', '').split(',')
            return f"TO_HEX(MD5(CONCAT({', '.join(f'CAST({col.strip()} AS STRING)' for col in columns)})))"

        return re.sub(
            r'{{\s*dbt_utils\.surrogate_key\(\[(.*?)\]\)\s*}}',
            replace_surrogate_key,
            content
        )

    def _convert_dbt_utils_date_functions(self, content: str) -> str:
        def replace_datediff(match):
            args = match.group(1).split(',')
            if len(args) != 3:
                return match.group(0)  # Return original if not 3 arguments
            part, start_date, end_date = [arg.strip() for arg in args]
            return f"DATE_DIFF({end_date}, {start_date}, {part})"

        def replace_dateadd(match):
            args = match.group(1).split(',')
            if len(args) != 3:
                return match.group(0)  # Return original if not 3 arguments
            part, number, date = [arg.strip() for arg in args]
            return f"DATE_ADD({date}, INTERVAL {number} {part})"

        def replace_date_trunc(match):
            args = match.group(1).split(',')
            if len(args) != 2:
                return match.group(0)  # Return original if not 2 arguments
            part, date = [arg.strip() for arg in args]
            return f"DATE_TRUNC({date}, {part})"

        def replace_date_part(match):
            args = match.group(1).split(',')
            if len(args) != 2:
                return match.group(0)  # Return original if not 2 arguments
            part, date = [arg.strip() for arg in args]
            return f"EXTRACT({part} FROM {date})"
        
        content = re.sub(r'{{\s*dbt\.datediff\((.*?)\)\s*}}', replace_datediff, content)
        content = re.sub(r'{{\s*dbt\.dateadd\((.*?)\)\s*}}', replace_dateadd, content)
        content = re.sub(r'{{\s*dbt\.date_trunc\((.*?)\)\s*}}', replace_date_trunc, content)
        content = re.sub(r'{{\s*dbt\.date_part\((.*?)\)\s*}}', replace_date_part, content)
        content = re.sub(r'{{\s*dbt_utils\.datediff\((.*?)\)\s*}}', replace_datediff, content)
        content = re.sub(r'{{\s*dbt_utils\.dateadd\((.*?)\)\s*}}', replace_dateadd, content)
        content = re.sub(r'{{\s*dbt_utils\.date_trunc\((.*?)\)\s*}}', replace_date_trunc, content)
        content = re.sub(r'{{\s*dbt_utils\.date_part\((.*?)\)\s*}}', replace_date_part, content)

        return content

    def _convert_dbt_utils_group_by(self, content: str) -> str:
        def replace_group_by(match):
            n = int(match.group(1).strip())
            return f"GROUP BY {', '.join(str(i) for i in range(1, n + 1))}"

        pattern = r'{{\s*dbt_utils\.group_by\((\d+)\)\s*}}'
        return re.sub(pattern, replace_group_by, content)

    def _convert_macros(self, content: str) -> str:
        # Convert dbt_utils type functions
        content = self._convert_dbt_utils_type_functions(content)
        
        # Convert dbt_utils.star
        content = self._convert_dbt_utils_star(content)
        
        # Convert dbt_utils.surrogate_key
        content = self._convert_dbt_utils_surrogate_key(content)
        
        # Convert dbt_utils date functions
        content = self._convert_dbt_utils_date_functions(content)
        
        # Convert dbt_utils.group_by
        content = self._convert_dbt_utils_group_by(content)
        
            # Convert source calls
        content = re.sub(
            r'\{\{\s*source\([\'"](\w+)[\'"]\s*,\s*[\'"](\w+)[\'"]\)\s*\}\}',
            r'${ref("\2")}',
            content
        )
        
        # This is a simplified macro conversion for other macros
        #macro_calls = re.findall(r'\{\{\s*(\w+)\((.*?)\)\s*\}\}', content)
        #for macro_name, macro_args in macro_calls:
        #    if not macro_name.startswith('dbt_utils.'):  # Skip if it's a dbt_utils macro as we've already handled specific ones
        #        content = content.replace(
        #            f"{{{{ {macro_name}({macro_args}) }}}}",
        #            f"${{ functions.{macro_name}({macro_args}) }}"
        #        )
        return content

    def _convert_comments(self, content: str) -> str:
        # Convert Jinja comments to JavaScript comments
        content = re.sub(r'\{#(.*?)#\}', r'/*\1*/', content, flags=re.DOTALL)
        return content

    def _convert_incremental(self, content: str) -> str:
        return content.replace('is_incremental()', 'incremental()')
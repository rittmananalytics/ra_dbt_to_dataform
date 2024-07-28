from pathlib import Path
import yaml

class SourceConverter:
    def __init__(self, dbt_project_path: Path, dataform_output_path: Path):
        self.dbt_project_path = dbt_project_path
        self.dataform_output_path = dataform_output_path
        self.project_config = self._load_project_config()

    def _load_project_config(self):
        try:
            with open(self.dbt_project_path / 'dbt_project.yml', 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            print(f"Error loading dbt_project.yml: {str(e)}")
            return {}

    def convert_sources(self):
        sources_dir = self.dataform_output_path / 'definitions' / 'sources'
        sources_dir.mkdir(parents=True, exist_ok=True)
        source_tables = set()

        model_yml_files = list(self.dbt_project_path.rglob('models/**/*.yml'))
        for yml_file in model_yml_files:
            try:
                with open(yml_file, 'r') as f:
                    yml_content = yaml.safe_load(f)
                
                if yml_content is None or not isinstance(yml_content, dict):
                    continue
                
                if 'sources' in yml_content:
                    for source in yml_content['sources']:
                        source_database = source.get('database')
                        source_schema = source.get('schema')
                        for table in source.get('tables', []):
                            self._create_source_file(source_database, source_schema, table)
                            source_tables.add(table['name'])
            except Exception as e:
                print(f"Error processing YAML file {yml_file}: {str(e)}")

        return source_tables

    def _create_source_file(self, source_database, source_schema, table):
        table_name = table['name']
        source_file = self.dataform_output_path / 'definitions' / 'sources' / f'{table_name}.sqlx'
        
        # Use source-specific database and schema if available, otherwise fall back to project defaults
        database = source_database or self.project_config.get('vars', {}).get('database', 'dataform.projectConfig.defaultDatabase')
        schema = source_schema or self.project_config.get('vars', {}).get('schema', 'dataform.projectConfig.defaultSchema')
        
        # Handle potential Jinja templating in dbt source definitions
        database = self._resolve_jinja_var(database)
        schema = self._resolve_jinja_var(schema)
        
        content = f"""
config {{
  type: "declaration",
  database: "{database}",
  schema: "{schema}",
  name: "{table_name}"
}}
        """
        
        source_file.write_text(content.strip())
        print(f"Created source file: {source_file}")

    def _resolve_jinja_var(self, value):
        # Simple Jinja variable resolution
        if isinstance(value, str) and value.startswith('{{') and value.endswith('}}'):
            var_name = value.strip('{{ }}').strip()
            return self.project_config.get('vars', {}).get(var_name, value)
        return value
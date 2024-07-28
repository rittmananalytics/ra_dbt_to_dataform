import yaml
import json
from pathlib import Path

class ProjectConfigConverter:
    def __init__(self, dbt_project_path: Path, dataform_config_path: Path):
        self.dbt_project_path = dbt_project_path
        self.dataform_config_path = dataform_config_path

    def convert(self):
        with open(self.dbt_project_path, 'r') as f:
            dbt_config = yaml.safe_load(f)

        # Try to determine the location from dbt project
        default_location = self._get_default_location(dbt_config)

        dataform_config = {
            "warehouse": "bigquery",
            "defaultDatabase": dbt_config.get('target', {}).get('project', 'default'),
            "defaultSchema": dbt_config.get('target-path', 'dataform'),
            "defaultLocation": default_location,
            "assertionSchema": "dataform_assertions",
            "vars": dbt_config.get('vars', {})
        }

        # Handle top-level vars
        if 'vars' in dbt_config:
            dataform_config['vars'].update(dbt_config['vars'])

        # Handle model-specific configurations and vars
        if 'models' in dbt_config:
            project_name = dbt_config['name']
            if project_name in dbt_config['models']:
                project_models = dbt_config['models'][project_name]
                
                for model_group, model_config in project_models.items():
                    if isinstance(model_config, dict):
                        # Handle schema and materialization
                        if 'schema' in model_config:
                            dataform_config['vars'][f'{model_group}_schema'] = model_config['schema']
                        if 'materialized' in model_config:
                            dataform_config['vars'][f'{model_group}_materialized'] = model_config['materialized']
                        
                        # Handle model-specific vars
                        if 'vars' in model_config:
                            for var_name, var_value in model_config['vars'].items():
                                dataform_config['vars'][f'{model_group}_{var_name}'] = var_value

        # Ensure the directory exists
        self.dataform_config_path.parent.mkdir(parents=True, exist_ok=True)

        # Write the Dataform config
        with open(self.dataform_config_path, 'w') as f:
            json.dump(dataform_config, f, indent=2)

        print(f"Dataform config written to {self.dataform_config_path}")
    def _get_default_location(self, dbt_config):
        # Try to get location from dbt project config
        location = dbt_config.get('target', {}).get('location')
        
        if not location:
            # If not found, check profiles.yml
            profiles_path = self.dbt_project_path.parent / 'profiles.yml'
            if profiles_path.exists():
                with open(profiles_path, 'r') as f:
                    profiles = yaml.safe_load(f)
                    profile_name = dbt_config.get('profile')
                    if profile_name:
                        location = profiles.get(profile_name, {}).get('outputs', {}).get('default', {}).get('location')

        # If still not found, use a default value
        return location or "europe-west2"  # or any other default you prefer
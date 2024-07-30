import yaml
import json
from pathlib import Path
import os
import re

import re
import os
import yaml
import json
from pathlib import Path

class ProjectConfigConverter:
    def __init__(self, dbt_project_path: Path, dataform_config_path: Path):
        self.dbt_project_path = dbt_project_path
        self.dataform_config_path = dataform_config_path

    def convert_source_to_ref(self, value):
        if isinstance(value, str):
            source_match = re.search(r'\{\{\s*source\([\'"](\w+)[\'"]\s*,\s*[\'"](\w+)[\'"]\)\s*\}\}', value)
            if source_match:
                source_name, table_name = source_match.groups()
                return f"${{ref('source_{table_name}')}}"
        return value

    def convert(self):
        with open(self.dbt_project_path, 'r') as f:
            dbt_config = yaml.safe_load(f)

        default_location = self._get_default_location(dbt_config)

        dataform_config = {
            "warehouse": "bigquery",
            "defaultDatabase": dbt_config.get('target', {}).get('project', 'default'),
            "defaultSchema": dbt_config.get('target-path', 'dataform'),
            "defaultLocation": default_location,
            "assertionSchema": "dataform_assertions",
            "vars": {}
        }

        js_vars = {}

        # Handle top-level vars and scoped vars
        if 'vars' in dbt_config:
            for scope, vars_dict in dbt_config['vars'].items():
                if isinstance(vars_dict, dict):
                    # This is a scoped variable set
                    js_vars[scope] = {k: self.convert_source_to_ref(v) for k, v in vars_dict.items()}
                else:
                    # This is a top-level variable
                    dataform_config['vars'][scope] = self.convert_source_to_ref(vars_dict)

        # Handle model-specific configurations
        if 'models' in dbt_config:
            for model_name, model_config in dbt_config['models'].items():
                if isinstance(model_config, dict):
                    for key, value in model_config.items():
                        if key.startswith('+'):
                            # This is a model configuration
                            dataform_config['vars'][f"{model_name}_{key[1:]}"] = value
                        elif key == 'vars':
                            # These are model-specific variables
                            for var_name, var_value in value.items():
                                dataform_config['vars'][f"{model_name}_{var_name}"] = self.convert_source_to_ref(var_value)

        # Ensure the directory exists
        os.makedirs(os.path.dirname(self.dataform_config_path), exist_ok=True)

        # Write the Dataform config
        with open(self.dataform_config_path, 'w') as f:
            json.dump(dataform_config, f, indent=2)

        print(f"Dataform config written to {self.dataform_config_path}")

        # Generate a JavaScript file for scoped variables
        if js_vars:
            js_content = "module.exports = " + json.dumps(js_vars, indent=2) + ";"
            js_path = self.dataform_config_path.parent / 'definitions.js'
            
            # Ensure the directory exists
            js_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(js_path, 'w') as f:
                f.write(js_content)
            print(f"Scoped variables written to {js_path}")

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
        return location or "europe-west2"  # or any other default you prefer_p
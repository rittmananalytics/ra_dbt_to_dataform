# metadata_converter.py

import yaml
from pathlib import Path

class MetadataConverter:
    def convert_schema_yml(self, schema_path: Path) -> str:
        with open(schema_path, 'r') as f:
            dbt_schema = yaml.safe_load(f)
        
        dataform_js = "module.exports = {\n"
        
        for model in dbt_schema.get('models', []):
            model_name = model['name']
            dataform_js += f"  {model_name}: (\n"
            dataform_js += f"    ctx: dataform.Context,\n"
            dataform_js += f"    ref: dataform.Ref\n"
            dataform_js += "  ) => ({\n"
            
            if 'description' in model:
                dataform_js += f"    description: \"{model['description']}\",\n"
            
            if 'columns' in model:
                dataform_js += "    columns: {\n"
                for column in model['columns']:
                    dataform_js += f"      {column['name']}: {{\n"
                    if 'description' in column:
                        dataform_js += f"        description: \"{column['description']}\",\n"
                    if 'tests' in column:
                        dataform_js += "        tests: [\n"
                        for test in column['tests']:
                            if isinstance(test, str):
                                dataform_js += f"          ctx.{test}(),\n"
                            elif isinstance(test, dict):
                                test_name = list(test.keys())[0]
                                test_params = test[test_name]
                                if isinstance(test_params, dict):
                                    params_str = ", ".join([f"{k}: {repr(v)}" for k, v in test_params.items()])
                                    dataform_js += f"          ctx.{test_name}({{{params_str}}}),\n"
                                else:
                                    dataform_js += f"          ctx.{test_name}(),\n"
                        dataform_js += "        ],\n"
                    dataform_js += "      },\n"
                dataform_js += "    },\n"
            
            dataform_js += "  }),\n"
        
        dataform_js += "};\n"
        
        return dataform_js
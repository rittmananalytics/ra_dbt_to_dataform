# project_generator.py

import os
import json
from pathlib import Path

class ProjectGenerator:
    def __init__(self, output_path: str):
        self.output_path = Path(output_path)

    def generate_project_structure(self):
        directories = [
            'definitions/sources',
            'definitions/intermediate',
            'definitions/output',
            'includes'
        ]
        for directory in directories:
            (self.output_path / directory).mkdir(parents=True, exist_ok=True)

        self._create_package_json()

    def _create_package_json(self):
        package_json = {
            "name": "dataform-project",
            "version": "1.0.0",
            "dependencies": {
                "@dataform/core": "2.0.1"
            }
        }
        
        with open(self.output_path / 'package.json', 'w') as f:
            json.dump(package_json, f, indent=2)
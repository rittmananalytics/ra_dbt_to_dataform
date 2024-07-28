import os
from pathlib import Path
from typing import Dict, List

class RepositoryAnalyzer:
    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path)
        self.dbt_project_path = self._find_dbt_project()
        
    def _find_dbt_project(self) -> Path:
        """Find the dbt_project.yml file to locate the project root."""
        for root, _, files in os.walk(self.repo_path):
            if 'dbt_project.yml' in files:
                return Path(root)
        raise FileNotFoundError("No dbt_project.yml found in the repository.")

    def analyze(self) -> Dict[str, List[Path]]:
        """Analyze the dbt project structure and return a dictionary of artifacts."""
        artifacts = {
            'models': [],
            'tests': [],
            'macros': [],
            'seeds': [],
            'analyses': [],
            'snapshots': [],
        }
        for artifact_type in artifacts:
            artifact_dir = self.dbt_project_path / artifact_type
            if artifact_dir.exists():
                if artifact_type == 'seeds':
                    artifacts[artifact_type] = list(artifact_dir.glob('*.csv'))
                else:
                    artifacts[artifact_type] = list(artifact_dir.rglob('*.sql'))
        
        # Add YAML files
        artifacts['yaml_files'] = list(self.dbt_project_path.rglob('*.yml'))
        
        return artifacts

    def get_project_config(self) -> Dict:
        """Read and return the dbt_project.yml configuration."""
        import yaml
        with open(self.dbt_project_path / 'dbt_project.yml', 'r') as f:
            return yaml.safe_load(f)

    def get_seed_files(self) -> List[Path]:
        """Get all seed files from the seeds directory."""
        seed_dir = self.dbt_project_path / 'seeds'
        return list(seed_dir.glob('*.csv')) if seed_dir.exists() else []

if __name__ == "__main__":
    # Example usage
    repo_path = "/path/to/local/dbt/repo"
    analyzer = RepositoryAnalyzer(repo_path)
    artifacts = analyzer.analyze()
    
    print("DBT Project Structure:")
    for artifact_type, files in artifacts.items():
        print(f"{artifact_type.capitalize()}:")
        for file in files:
            print(f"  - {file.relative_to(analyzer.dbt_project_path)}")
    
    print("\nProject Config:")
    print(analyzer.get_project_config())

    print("\nSeed Files:")
    for seed_file in analyzer.get_seed_files():
        print(f"  - {seed_file.relative_to(analyzer.dbt_project_path)}")
# conversion_report.py

from pathlib import Path
import json

class ConversionReport:
    def __init__(self, output_path: Path):
        self.output_path = output_path
        self.issues = []

    def add_issue(self, file_path: str, issue_type: str, description: str):
        self.issues.append({
            "file": file_path,
            "type": issue_type,
            "description": description
        })

    def generate_report(self):
        report = {
            "total_issues": len(self.issues),
            "issues": self.issues
        }

        report_file = self.output_path / "conversion_report.json"
        with open(report_file, "w") as f:
            json.dump(report, f, indent=2)

        print(f"Conversion report generated: {report_file}")

        # Also generate a human-readable summary
        summary_file = self.output_path / "conversion_summary.txt"
        with open(summary_file, "w") as f:
            f.write("Dataform Conversion Summary\n")
            f.write("===========================\n\n")
            f.write(f"Total issues found: {len(self.issues)}\n\n")
            
            if self.issues:
                f.write("Issues that need attention:\n")
                for issue in self.issues:
                    f.write(f"\nFile: {issue['file']}\n")
                    f.write(f"Type: {issue['type']}\n")
                    f.write(f"Description: {issue['description']}\n")
            else:
                f.write("No issues found. However, please review the converted project thoroughly.\n")

        print(f"Conversion summary generated: {summary_file}")
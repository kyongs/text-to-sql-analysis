# src/utils/semantic_layer.py
"""
Semantic Layer Utility
- Loads semantic layer JSON
- Applies disambiguation rules and schema additions to questions
"""

import os
import re
import json
from typing import Optional


class SemanticLayer:
    """Semantic layer for enhancing NLQ understanding"""

    def __init__(self, json_path: str = None):
        self.schema_additions = []
        self.disambiguation = []
        self.loaded = False

        if json_path and os.path.exists(json_path):
            self.load(json_path)

    def load(self, json_path: str):
        """Load semantic layer from JSON file"""
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)

        self.schema_additions = data.get('schema_additions', [])
        self.disambiguation = data.get('disambiguation', [])
        self.loaded = True

    def get_matching_rules(self, question: str) -> dict:
        """
        Find all matching rules for a given question

        Returns:
            {
                "schema_additions": [...],  # matching schema additions
                "disambiguation": [...],    # matching disambiguation rules
                "hints_for_extraction": [...],  # hints for fragment extraction
                "hints_for_main": [...]     # hints for main pipeline
            }
        """
        question_lower = question.lower()

        result = {
            "schema_additions": [],
            "disambiguation": [],
            "hints_for_extraction": [],
            "hints_for_main": []
        }

        # Check schema additions
        for addition in self.schema_additions:
            phrase = addition.get('phrase', '').lower()
            if phrase and phrase in question_lower:
                result["schema_additions"].append(addition)
                # Also add as main hint
                hint = f"- '{addition['phrase']}' refers to {addition['column']}"
                if addition.get('note'):
                    hint += f" ({addition['note']})"
                result["hints_for_main"].append(hint)

        # Check disambiguation rules
        for rule in self.disambiguation:
            pattern = rule.get('pattern', '')
            if pattern:
                try:
                    if re.search(pattern, question_lower, re.IGNORECASE):
                        result["disambiguation"].append(rule)

                        if rule.get('hint_for_extraction'):
                            result["hints_for_extraction"].append(rule['hint_for_extraction'])

                        if rule.get('hint_for_main'):
                            result["hints_for_main"].append(rule['hint_for_main'])
                except re.error:
                    # Invalid regex pattern, skip
                    pass

        return result

    def format_extraction_hint(self, question: str) -> Optional[str]:
        """
        Get formatted hint for fragment extraction
        Returns None if no matching rules
        """
        rules = self.get_matching_rules(question)
        hints = rules.get("hints_for_extraction", [])

        if not hints:
            return None

        return "\n".join([f"[Semantic Hint] {h}" for h in hints])

    def format_main_hint(self, question: str) -> Optional[str]:
        """
        Get formatted hint for main pipeline
        Returns None if no matching rules
        """
        rules = self.get_matching_rules(question)
        hints = rules.get("hints_for_main", [])

        if not hints:
            return None

        return "Additional Schema Context:\n" + "\n".join(hints)


def load_semantic_layer(dataset_path: str) -> Optional[SemanticLayer]:
    """
    Load semantic layer from dataset path
    Looks for semantic_layer.json in the dataset directory
    """
    json_path = os.path.join(dataset_path, "semantic_layer.json")

    if os.path.exists(json_path):
        return SemanticLayer(json_path)

    return None


# Test
if __name__ == "__main__":
    # Test with sample questions
    layer = SemanticLayer("data/beaver/dw/semantic_layer.json")

    test_questions = [
        "Show the percentage of the room area over the assignable floor area and building.",
        "What is the building name and floor number?",
        "Calculate the ratio of room area to assignable building area.",
    ]

    for q in test_questions:
        print(f"\nQ: {q}")
        rules = layer.get_matching_rules(q)

        if rules["schema_additions"]:
            print(f"  Schema additions: {len(rules['schema_additions'])}")
        if rules["disambiguation"]:
            print(f"  Disambiguation rules: {len(rules['disambiguation'])}")
        if rules["hints_for_extraction"]:
            print(f"  Extraction hints: {rules['hints_for_extraction']}")
        if rules["hints_for_main"]:
            print(f"  Main hints: {rules['hints_for_main']}")

        if not any([rules["schema_additions"], rules["disambiguation"]]):
            print("  (no matching rules)")

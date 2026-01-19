import re
import yaml
from typing import Dict, Optional, List
from dataclasses import dataclass

@dataclass
class SemanticType:
    name: str
    description: str
    regex_pattern: str
    is_pii: bool
    spark_type: str

class MetadataRegistry:
    """
    Central Repository for Entity Definitions and Semantic Types.
    Parses semantic_types.yaml and provides validation methods.
    """
    
    def __init__(self, config_path: str = None):
        if config_path is None:
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            self.config_path = os.path.join(base_dir, "config", "semantic_types.yaml")
        else:
            self.config_path = config_path
            
        self._types: Dict[str, SemanticType] = {}
        self._load_registry()

    def _load_registry(self):
# ... (rest of class)

# Singleton Instance for simulated static access
_registry_instance = None

def get_registry(path: str = None) -> MetadataRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = MetadataRegistry(path)
    return _registry_instance
        """Loads semantic definitions from YAML configuration."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            for type_def in config.get('types', []):
                self._types[type_def['name']] = SemanticType(
                    name=type_def['name'],
                    description=type_def['description'],
                    regex_pattern=type_def['regex'],
                    is_pii=type_def['pii'],
                    spark_type=type_def['data_type']
                )
            print(f"INFO: Successfully loaded {len(self._types)} semantic types from registry.")
        except FileNotFoundError:
            print(f"WARN: Semantic config not found at {self.config_path}. Registry is empty.")
        except Exception as e:
            print(f"ERROR: Failed to load registry: {str(e)}")

    def get_type(self, type_name: str) -> Optional[SemanticType]:
        """Retrieves a specific Semantic Type definition."""
        return self._types.get(type_name.upper())

    def validate(self, value: str, type_name: str) -> bool:
        """
        Validates a value against the regex of a semantic type.
        Returns False if type does not exist or pattern doesn't match.
        """
        if not value:
            return False
            
        semantic_type = self.get_type(type_name)
        if not semantic_type:
            return False
            
        return bool(re.match(semantic_type.regex_pattern, str(value)))

    def infer_semantic_type(self, sample_value: str) -> Optional[str]:
        """
        Attempts to infer the semantic type of a value by testing against all regex patterns.
        Returns the first matching type name, or None.
        """
        if not sample_value:
            return None
            
        for type_name, s_type in self._types.items():
            if re.match(s_type.regex_pattern, str(sample_value)):
                return type_name
        return None

# Singleton Instance for simulated static access
_registry_instance = None

def get_registry(path: str = None) -> MetadataRegistry:
    global _registry_instance
    if _registry_instance is None:
        _registry_instance = MetadataRegistry(path)
    return _registry_instance

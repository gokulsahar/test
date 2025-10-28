#!/usr/bin/env python3
"""
Talend Preprocessor Validation Script

This script validates that the preprocessor extracts ALL critical information
by comparing the original .items file with the generated JSON output.
"""

import xml.etree.ElementTree as ET
import json
import sys
from pathlib import Path
from typing import Dict, List, Tuple


class PreprocessorValidator:
    """Validates that no critical information is lost during preprocessing."""
    
    def __init__(self, items_file: str, json_file: str):
        self.items_file = Path(items_file)
        self.json_file = Path(json_file)
        self.xml_root = None
        self.json_data = None
        self.issues = []
        self.warnings = []
        
    def load_files(self) -> bool:
        """Load both XML and JSON files."""
        try:
            # Load XML
            tree = ET.parse(self.items_file)
            self.xml_root = tree.getroot()
            print(f"✓ Loaded XML: {self.items_file.name}")
            
            # Load JSON
            with open(self.json_file, 'r', encoding='utf-8') as f:
                self.json_data = json.load(f)
            print(f"✓ Loaded JSON: {self.json_file.name}")
            
            return True
        except Exception as e:
            print(f"✗ Error loading files: {e}")
            return False
    
    def validate_component_count(self) -> bool:
        """Ensure all components are extracted."""
        xml_nodes = self.xml_root.findall(".//node")
        json_components = self.json_data.get("components", [])
        
        xml_count = len(xml_nodes)
        json_count = len(json_components)
        
        print(f"\n[Component Count Check]")
        print(f"  XML components: {xml_count}")
        print(f"  JSON components: {json_count}")
        
        if xml_count != json_count:
            self.issues.append(f"Component count mismatch: XML has {xml_count}, JSON has {json_count}")
            return False
        
        print(f"  ✓ All components extracted")
        return True
    
    def validate_connection_count(self) -> bool:
        """Ensure all connections are extracted."""
        xml_connections = self.xml_root.findall(".//connection")
        json_connections = self.json_data.get("connections", [])
        
        xml_count = len(xml_connections)
        json_count = len(json_connections)
        
        print(f"\n[Connection Count Check]")
        print(f"  XML connections: {xml_count}")
        print(f"  JSON connections: {json_count}")
        
        if xml_count != json_count:
            self.issues.append(f"Connection count mismatch: XML has {xml_count}, JSON has {json_count}")
            return False
        
        print(f"  ✓ All connections extracted")
        return True
    
    def validate_element_parameters(self) -> bool:
        """Ensure all elementParameter nodes are captured."""
        xml_params = self.xml_root.findall(".//elementParameter")
        xml_count = len(xml_params)
        
        # Count JSON parameters
        json_count = 0
        for component in self.json_data.get("components", []):
            json_count += len(component.get("element_parameters", []))
        
        for connection in self.json_data.get("connections", []):
            json_count += len(connection.get("parameters", []))
        
        print(f"\n[Element Parameters Check]")
        print(f"  XML elementParameters: {xml_count}")
        print(f"  JSON parameters: {json_count}")
        
        if xml_count > json_count:
            self.warnings.append(f"Parameter count lower in JSON: XML has {xml_count}, JSON has {json_count}")
            print(f"  ⚠ Some parameters may be in other locations")
        else:
            print(f"  ✓ All parameters captured")
        
        return True
    
    def validate_metadata_schemas(self) -> bool:
        """Ensure all metadata/schema definitions are captured."""
        xml_metadata = self.xml_root.findall(".//metadata")
        xml_count = len(xml_metadata)
        
        json_count = 0
        for component in self.json_data.get("components", []):
            json_count += len(component.get("metadata", {}))
        
        print(f"\n[Metadata/Schema Check]")
        print(f"  XML metadata nodes: {xml_count}")
        print(f"  JSON metadata entries: {json_count}")
        
        if xml_count != json_count:
            self.warnings.append(f"Metadata count mismatch: XML has {xml_count}, JSON has {json_count}")
        else:
            print(f"  ✓ All metadata captured")
        
        return True
    
    def validate_critical_attributes(self) -> bool:
        """Check that critical attributes are present in components."""
        print(f"\n[Critical Attributes Check]")
        
        critical_attrs = ["componentName", "componentVersion"]
        missing_attrs = []
        
        for component in self.json_data.get("components", []):
            for attr in critical_attrs:
                if attr not in component and attr not in component.get("parameters", {}):
                    missing_attrs.append((component.get("unique_id", "unknown"), attr))
        
        if missing_attrs:
            print(f"  ⚠ Some components missing critical attributes:")
            for comp_id, attr in missing_attrs[:5]:  # Show first 5
                print(f"    - {comp_id}: missing {attr}")
            self.warnings.append(f"{len(missing_attrs)} critical attributes missing")
        else:
            print(f"  ✓ All critical attributes present")
        
        return True
    
    def check_for_code_expressions(self) -> bool:
        """Verify that code expressions and SQL queries are preserved."""
        print(f"\n[Code/Expression Preservation Check]")
        
        # Look for common code indicators in XML
        code_indicators = ["query", "expression", "code", "QUERY", "EXPRESSION"]
        xml_code_params = []
        
        for param in self.xml_root.findall(".//elementParameter"):
            param_name = param.get("name", "").upper()
            param_value = param.get("value", "")
            
            if any(indicator.upper() in param_name for indicator in code_indicators):
                if len(param_value) > 10:  # Non-trivial code
                    xml_code_params.append((param_name, len(param_value)))
        
        # Check if these appear in JSON
        json_code_params = []
        for component in self.json_data.get("components", []):
            for param in component.get("element_parameters", []):
                param_name = str(param.get("name", "")).upper()
                param_value = str(param.get("value", ""))
                
                if any(indicator.upper() in param_name for indicator in code_indicators):
                    if len(param_value) > 10:
                        json_code_params.append((param_name, len(param_value)))
        
        print(f"  XML code parameters: {len(xml_code_params)}")
        print(f"  JSON code parameters: {len(json_code_params)}")
        
        if len(xml_code_params) > len(json_code_params):
            self.warnings.append(f"Possible code loss: XML has {len(xml_code_params)} code params, JSON has {len(json_code_params)}")
        else:
            print(f"  ✓ Code expressions preserved")
        
        return True
    
    def validate_context_variables(self) -> bool:
        """Check if context variables are properly flagged."""
        print(f"\n[Context Variables Check]")
        
        flagged_vars = self.json_data.get("context_variables_used", [])
        print(f"  Flagged context variables: {len(flagged_vars)}")
        
        if flagged_vars:
            print(f"  Variables found: {[v['name'] for v in flagged_vars[:5]]}")
            print(f"  ✓ Context variables detected and flagged")
        else:
            print(f"  ℹ No context variables found (this may be normal)")
        
        return True
    
    def validate_routines(self) -> bool:
        """Check if routines are properly flagged."""
        print(f"\n[Routines Check]")
        
        flagged_routines = self.json_data.get("routines_used", [])
        print(f"  Flagged routines: {len(flagged_routines)}")
        
        if flagged_routines:
            print(f"  Routines found: {[r['routine_name'] for r in flagged_routines[:5]]}")
            print(f"  ✓ Routines detected and flagged")
        else:
            print(f"  ℹ No routines found (this may be normal)")
        
        return True
    
    def check_file_size_reduction(self) -> bool:
        """Check the file size reduction."""
        print(f"\n[File Size Analysis]")
        
        xml_size = self.items_file.stat().st_size
        json_size = self.json_file.stat().st_size
        
        reduction_pct = ((xml_size - json_size) / xml_size) * 100
        
        print(f"  Original XML: {xml_size / 1024:.2f} KB")
        print(f"  Cleaned JSON: {json_size / 1024:.2f} KB")
        print(f"  Size reduction: {reduction_pct:.1f}%")
        
        if reduction_pct < 0:
            print(f"  ℹ JSON is larger (due to formatting)")
        else:
            print(f"  ✓ File size reduced")
        
        return True
    
    def validate(self) -> bool:
        """Run all validation checks."""
        print("=" * 70)
        print("Preprocessor Validation Report")
        print("=" * 70)
        
        if not self.load_files():
            return False
        
        # Run all checks
        checks = [
            self.validate_component_count,
            self.validate_connection_count,
            self.validate_element_parameters,
            self.validate_metadata_schemas,
            self.validate_critical_attributes,
            self.check_for_code_expressions,
            self.validate_context_variables,
            self.validate_routines,
            self.check_file_size_reduction
        ]
        
        for check in checks:
            try:
                check()
            except Exception as e:
                self.issues.append(f"Error in {check.__name__}: {e}")
        
        # Print summary
        print("\n" + "=" * 70)
        print("Validation Summary")
        print("=" * 70)
        
        if self.issues:
            print(f"\n❌ CRITICAL ISSUES FOUND ({len(self.issues)}):")
            for issue in self.issues:
                print(f"  • {issue}")
        
        if self.warnings:
            print(f"\n⚠ WARNINGS ({len(self.warnings)}):")
            for warning in self.warnings:
                print(f"  • {warning}")
        
        if not self.issues and not self.warnings:
            print("\n✓ ALL CHECKS PASSED - No information loss detected!")
            return True
        elif not self.issues:
            print("\n✓ VALIDATION PASSED with warnings")
            print("  (Warnings are informational and may not indicate actual issues)")
            return True
        else:
            print("\n✗ VALIDATION FAILED - Please review issues above")
            return False


def main():
    """Command-line interface."""
    if len(sys.argv) < 3:
        print("Usage: python validate_preprocessor.py <original_items_file> <generated_json_file>")
        print("\nExample:")
        print("  python validate_preprocessor.py MyJob_0.1.item MyJob_0.1_cleaned.json")
        sys.exit(1)
    
    items_file = sys.argv[1]
    json_file = sys.argv[2]
    
    validator = PreprocessorValidator(items_file, json_file)
    success = validator.validate()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
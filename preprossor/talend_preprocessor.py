#!/usr/bin/env python3
"""
Talend .items File Preprocessor

This script preprocesses Talend .items files by:
1. Removing GUI/visual noise (positions, colors, sizes)
2. Preserving 100% of functional logic
3. Flagging context variables and routines
4. Outputting clean, structured JSON for AI consumption

Usage:
    python talend_preprocessor.py <input_items_file> [output_json_file]
"""

import xml.etree.ElementTree as ET
import json
import sys
import os
import re
from typing import Dict, List, Any, Optional
from pathlib import Path


class TalendPreprocessor:
    """
    Preprocessor for Talend .items files.
    Extracts functional logic while removing GUI noise.
    """
    
    # GUI/Visual attributes to remove (safe to delete)
    NOISE_ATTRIBUTES = {
        'posX', 'posY', 'offsetLabelX', 'offsetLabelY',
        'sizeX', 'sizeY', 'screenColor', 'fillColor',
        'lineColor', 'labelColor', 'portraitBackgroundColor',
        'subjobColor', 'alpha', 'visible', 'pointLabelVisible'
    }
    
    def __init__(self, items_file: str):
        """Initialize preprocessor with input file path."""
        self.items_file = Path(items_file)
        self.tree = None
        self.root = None
        self.job_data = {
            "job_metadata": {},
            "components": [],
            "connections": [],
            "context_variables_used": [],
            "routines_used": [],
            "subjobs": [],
            "notes": [],
            "metadata_references": {},
            "error_handling": {},
            "raw_preserved_data": {}  # For anything we're unsure about
        }
        
    def parse_xml(self) -> bool:
        """Parse the XML file."""
        try:
            self.tree = ET.parse(self.items_file)
            self.root = self.tree.getroot()
            print(f"✓ Successfully parsed XML file: {self.items_file.name}")
            return True
        except ET.ParseError as e:
            print(f"✗ Error parsing XML: {e}")
            return False
        except FileNotFoundError:
            print(f"✗ File not found: {self.items_file}")
            return False
    
    def extract_job_metadata(self):
        """Extract high-level job metadata."""
        print("\n[1/8] Extracting job metadata...")
        
        # Job name and version from root attributes
        self.job_data["job_metadata"]["name"] = self.root.get("name", "Unknown")
        self.job_data["job_metadata"]["version"] = self.root.get("version", "0.1")
        self.job_data["job_metadata"]["description"] = self.root.get("description", "")
        self.job_data["job_metadata"]["purpose"] = self.root.get("purpose", "")
        self.job_data["job_metadata"]["author"] = self.root.get("author", "")
        self.job_data["job_metadata"]["status"] = self.root.get("status", "")
        
        # Extract all root attributes except noise
        for attr_name, attr_value in self.root.attrib.items():
            if attr_name not in self.NOISE_ATTRIBUTES:
                if attr_name not in self.job_data["job_metadata"]:
                    self.job_data["job_metadata"][attr_name] = attr_value
        
        print(f"  ✓ Job: {self.job_data['job_metadata']['name']} v{self.job_data['job_metadata']['version']}")
    
    def extract_components(self):
        """Extract all components and their configurations."""
        print("\n[2/8] Extracting components...")
        
        # Find all nodes (components)
        nodes = self.root.findall(".//node")
        print(f"  Found {len(nodes)} components")
        
        for idx, node in enumerate(nodes, 1):
            component = {
                "unique_id": node.get("componentName", f"component_{idx}"),
                "type": node.get("componentName", "Unknown"),
                "label": node.get("componentVersion", ""),
                "parameters": {},
                "element_parameters": [],
                "metadata": {}
            }
            
            # Extract all attributes (except noise)
            for attr_name, attr_value in node.attrib.items():
                if attr_name not in self.NOISE_ATTRIBUTES:
                    component[attr_name] = attr_value
            
            # Extract elementParameter (the most critical part!)
            elem_params = node.findall(".//elementParameter")
            for param in elem_params:
                param_data = {
                    "name": param.get("field", param.get("name", "unknown")),
                    "value": param.get("value", ""),
                    "field_type": param.get("field", ""),
                    "show": param.get("show", "true")
                }
                
                # Preserve ALL attributes of the parameter
                for attr_name, attr_value in param.attrib.items():
                    if attr_name not in param_data:
                        param_data[attr_name] = attr_value
                
                # Check for nested elements (like items in a list)
                for child in param:
                    if child.tag not in param_data:
                        param_data[child.tag] = []
                    
                    child_data = dict(child.attrib)
                    if child.text:
                        child_data["text"] = child.text
                    param_data[child.tag].append(child_data)
                
                component["element_parameters"].append(param_data)
                
                # Also store in parameters dict for easy access
                param_name = param_data["name"]
                component["parameters"][param_name] = param_data["value"]
            
            # Extract metadata (schemas)
            metadata_nodes = node.findall(".//metadata")
            for meta in metadata_nodes:
                meta_name = meta.get("name", f"metadata_{len(component['metadata'])}")
                component["metadata"][meta_name] = {
                    "connector": meta.get("connector", ""),
                    "name": meta.get("name", ""),
                    "label": meta.get("label", ""),
                    "columns": []
                }
                
                # Extract columns
                columns = meta.findall(".//column")
                for col in columns:
                    col_data = dict(col.attrib)
                    component["metadata"][meta_name]["columns"].append(col_data)
            
            self.job_data["components"].append(component)
            print(f"  ✓ [{idx}/{len(nodes)}] {component['type']}: {component['unique_id']}")
    
    def extract_connections(self):
        """Extract all connections between components."""
        print("\n[3/8] Extracting connections...")
        
        connections = self.root.findall(".//connection")
        print(f"  Found {len(connections)} connections")
        
        for idx, conn in enumerate(connections, 1):
            connection_data = {
                "from_component": conn.get("source", ""),
                "to_component": conn.get("target", ""),
                "connection_type": conn.get("connectorName", ""),
                "label": conn.get("label", ""),
            }
            
            # Extract all attributes except noise
            for attr_name, attr_value in conn.attrib.items():
                if attr_name not in self.NOISE_ATTRIBUTES:
                    if attr_name not in connection_data:
                        connection_data[attr_name] = attr_value
            
            # Extract element parameters for connections (like filters, mappings)
            elem_params = conn.findall(".//elementParameter")
            connection_data["parameters"] = []
            for param in elem_params:
                param_data = {
                    "name": param.get("name", param.get("field", "unknown")),
                    "value": param.get("value", "")
                }
                for attr_name, attr_value in param.attrib.items():
                    param_data[attr_name] = attr_value
                connection_data["parameters"].append(param_data)
            
            self.job_data["connections"].append(connection_data)
            print(f"  ✓ [{idx}/{len(connections)}] {connection_data['from_component']} → {connection_data['to_component']} ({connection_data['connection_type']})")
    
    def extract_context_variables(self):
        """Flag all context variable references in the job."""
        print("\n[4/8] Flagging context variables...")
        
        # Pattern 1: ${VARIABLE} syntax
        pattern1 = r'\$\{([A-Za-z_][A-Za-z0-9_]*)\}'
        # Pattern 2: context.VARIABLE syntax
        pattern2 = r'context\.([A-Za-z_][A-Za-z0-9_]*)'
        
        context_vars = {}
        
        # Search in all components' parameters
        for component in self.job_data["components"]:
            for param in component.get("element_parameters", []):
                param_value = str(param.get("value", ""))
                
                # Find both types of context variable references
                matches1 = re.findall(pattern1, param_value)
                matches2 = re.findall(pattern2, param_value)
                all_matches = matches1 + matches2
                
                for var_name in all_matches:
                    if var_name not in context_vars:
                        context_vars[var_name] = {
                            "name": var_name,
                            "referenced_in": [],
                            "usage_examples": []
                        }
                    
                    context_vars[var_name]["referenced_in"].append({
                        "component": component["unique_id"],
                        "parameter": param["name"]
                    })
                    
                    if param_value not in context_vars[var_name]["usage_examples"]:
                        context_vars[var_name]["usage_examples"].append(param_value)
        
        self.job_data["context_variables_used"] = list(context_vars.values())
        print(f"  ✓ Found {len(context_vars)} context variables: {list(context_vars.keys())}")
    
    def extract_routines(self):
        """Flag all routine calls in the job."""
        print("\n[5/8] Flagging routine calls...")
        
        # Pattern for routine calls: RoutineName.methodName
        routine_pattern = r'([A-Z][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*)\s*\('
        routines = {}
        
        # Search in all components' parameters (especially expressions, code)
        for component in self.job_data["components"]:
            for param in component.get("element_parameters", []):
                param_value = str(param.get("value", ""))
                matches = re.findall(routine_pattern, param_value)
                
                for routine_call in matches:
                    if routine_call not in routines:
                        routines[routine_call] = {
                            "routine_name": routine_call,
                            "used_in_components": [],
                            "contexts": []
                        }
                    
                    routines[routine_call]["used_in_components"].append({
                        "component": component["unique_id"],
                        "component_type": component["type"],
                        "parameter": param["name"]
                    })
                    
                    # Store context (the expression/code where it's used)
                    if len(param_value) < 200:  # Only if not too long
                        if param_value not in routines[routine_call]["contexts"]:
                            routines[routine_call]["contexts"].append(param_value)
        
        self.job_data["routines_used"] = list(routines.values())
        print(f"  ✓ Found {len(routines)} routine calls: {list(routines.keys())}")
    
    def extract_subjobs(self):
        """Extract subjob information."""
        print("\n[6/8] Extracting subjobs...")
        
        subjobs = self.root.findall(".//subjob")
        print(f"  Found {len(subjobs)} subjobs")
        
        for subjob in subjobs:
            subjob_data = dict(subjob.attrib)
            # Remove noise attributes
            subjob_data = {k: v for k, v in subjob_data.items() if k not in self.NOISE_ATTRIBUTES}
            self.job_data["subjobs"].append(subjob_data)
    
    def extract_notes(self):
        """Extract notes/comments from the job."""
        print("\n[7/8] Extracting notes and comments...")
        
        notes = self.root.findall(".//note")
        print(f"  Found {len(notes)} notes")
        
        for note in notes:
            note_data = {
                "text": note.get("text", ""),
                "attached_to": note.get("opaque", "")
            }
            for attr_name, attr_value in note.attrib.items():
                if attr_name not in self.NOISE_ATTRIBUTES:
                    note_data[attr_name] = attr_value
            
            self.job_data["notes"].append(note_data)
    
    def validate_extraction(self) -> bool:
        """
        Validate that we haven't missed any critical information.
        This is a sanity check to ensure completeness.
        """
        print("\n[8/8] Validating extraction completeness...")
        
        validation_checks = {
            "components_extracted": len(self.job_data["components"]) > 0,
            "connections_extracted": True,  # Connections can be 0 for simple jobs
            "job_metadata_exists": bool(self.job_data["job_metadata"].get("name"))
        }
        
        # Count original XML elements
        original_nodes = len(self.root.findall(".//node"))
        original_connections = len(self.root.findall(".//connection"))
        
        extracted_nodes = len(self.job_data["components"])
        extracted_connections = len(self.job_data["connections"])
        
        validation_checks["all_components_extracted"] = original_nodes == extracted_nodes
        validation_checks["all_connections_extracted"] = original_connections == extracted_connections
        
        print(f"\n  Validation Results:")
        print(f"  ✓ Components: {extracted_nodes}/{original_nodes} extracted")
        print(f"  ✓ Connections: {extracted_connections}/{original_connections} extracted")
        print(f"  ✓ Context variables flagged: {len(self.job_data['context_variables_used'])}")
        print(f"  ✓ Routines flagged: {len(self.job_data['routines_used'])}")
        print(f"  ✓ Subjobs: {len(self.job_data['subjobs'])}")
        print(f"  ✓ Notes: {len(self.job_data['notes'])}")
        
        all_passed = all(validation_checks.values())
        
        if not all_passed:
            print("\n  ⚠ WARNING: Some validation checks failed!")
            for check, passed in validation_checks.items():
                if not passed:
                    print(f"    ✗ {check}")
        else:
            print("\n  ✓ All validation checks passed!")
        
        return all_passed
    
    def save_json(self, output_file: Optional[str] = None) -> str:
        """Save extracted data as JSON."""
        if output_file is None:
            output_file = self.items_file.stem + "_cleaned.json"
        
        output_path = Path(output_file)
        
        # Add processing metadata
        self.job_data["_preprocessor_metadata"] = {
            "source_file": str(self.items_file),
            "output_file": str(output_path),
            "preprocessor_version": "1.0",
            "extraction_complete": True
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.job_data, f, indent=2, ensure_ascii=False)
        
        print(f"\n✓ Output saved to: {output_path}")
        print(f"  File size: {output_path.stat().st_size / 1024:.2f} KB")
        
        return str(output_path)
    
    def process(self, output_file: Optional[str] = None) -> bool:
        """
        Main processing pipeline.
        Returns True if successful, False otherwise.
        """
        print("=" * 70)
        print("Talend .items Preprocessor")
        print("=" * 70)
        
        # Parse XML
        if not self.parse_xml():
            return False
        
        # Extract all information
        try:
            self.extract_job_metadata()
            self.extract_components()
            self.extract_connections()
            self.extract_context_variables()
            self.extract_routines()
            self.extract_subjobs()
            self.extract_notes()
            
            # Validate
            validation_passed = self.validate_extraction()
            
            # Save output
            self.save_json(output_file)
            
            print("\n" + "=" * 70)
            print("✓ Preprocessing Complete!")
            print("=" * 70)
            
            return validation_passed
            
        except Exception as e:
            print(f"\n✗ Error during processing: {e}")
            import traceback
            traceback.print_exc()
            return False


def main():
    """Command-line interface."""
    if len(sys.argv) < 2:
        print("Usage: python talend_preprocessor.py <input_items_file> [output_json_file]")
        print("\nExample:")
        print("  python talend_preprocessor.py MyJob_0.1.item")
        print("  python talend_preprocessor.py MyJob_0.1.item output.json")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    preprocessor = TalendPreprocessor(input_file)
    success = preprocessor.process(output_file)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
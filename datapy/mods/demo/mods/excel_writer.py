"""
Excel Writer Mod for DataPy Framework.

Writes pandas DataFrame to Excel files with basic formatting.
"""

import pandas as pd
import os
from pathlib import Path
from typing import Dict, Any

from datapy.mod_manager.result import ModResult
from datapy.mod_manager.base import ModMetadata, ConfigSchema
from datapy.mod_manager.logger import setup_logger

# Required metadata
METADATA = ModMetadata(
    type="excel_writer",
    version="1.0.0",
    description="Writes pandas DataFrame to Excel files with basic formatting",
    category="sink",
    input_ports=["data"],
    output_ports=[],
    globals=["output_path", "rows_written", "file_size"],
    packages=["pandas>=1.5.0", "openpyxl>=3.0.0"],
    python_version=">=3.8"
)

# Parameter schema
CONFIG_SCHEMA = ConfigSchema(
    required={
        "data": {
            "type": "object",
            "description": "Input DataFrame to write to Excel"
        },
        "output_path": {
            "type": "str",
            "description": "Path where Excel file will be written"
        }
    },
    optional={
        "sheet_name": {
            "type": "str",
            "default": "Data",
            "description": "Name of the Excel sheet"
        },
        "include_index": {
            "type": "bool",
            "default": False,
            "description": "Include DataFrame index in output"
        },
        "create_directories": {
            "type": "bool",
            "default": True,
            "description": "Create parent directories if they don't exist"
        },
        "add_title": {
            "type": "str",
            "default": None,
            "description": "Add title row at the top (optional)"
        }
    }
)


def run(params: Dict[str, Any]) -> Dict[str, Any]:
    """
    Execute Excel writer with given parameters.
    
    Args:
        params: Dictionary containing data and output configuration
        
    Returns:
        ModResult dictionary with write results, metrics, and status
    """
    # Extract mod context
    mod_name = params.get("_mod_name", "excel_writer")
    mod_type = params.get("_mod_type", "excel_writer")
    
    # Setup logging with mod context
    logger = setup_logger(__name__, mod_type, mod_name)
    
    # Initialize result
    result = ModResult(mod_type, mod_name)
    
    try:
        # Extract parameters
        data = params["data"]
        output_path = params["output_path"]
        sheet_name = params.get("sheet_name", "Data")
        include_index = params.get("include_index", False)
        create_directories = params.get("create_directories", True)
        add_title = params.get("add_title", None)
        
        logger.info(f"Starting Excel write", extra={
            "output_path": output_path,
            "sheet_name": sheet_name,
            "input_rows": len(data) if hasattr(data, '__len__') else 'unknown'
        })
        
        # Validate input data
        if not isinstance(data, pd.DataFrame):
            error_msg = f"Input data must be a pandas DataFrame, got {type(data)}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
        
        # Convert output_path to Path object
        output_file = Path(output_path)
        
        # Ensure .xlsx extension
        if not output_file.suffix.lower() in ['.xlsx', '.xls']:
            output_file = output_file.with_suffix('.xlsx')
            logger.info(f"Added .xlsx extension: {output_file}")
        
        # Create parent directories if requested and needed
        if create_directories and not output_file.parent.exists():
            try:
                output_file.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directories: {output_file.parent}")
            except Exception as e:
                error_msg = f"Failed to create directories {output_file.parent}: {e}"
                logger.error(error_msg)
                result.add_error(error_msg)
                return result.error()
        
        # Validate data is not empty (warning, not error)
        if data.empty:
            warning_msg = "Input DataFrame is empty, writing empty Excel file"
            logger.warning(warning_msg)
            result.add_warning(warning_msg)
        
        try:
            # Write to Excel with basic formatting
            with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
                start_row = 0
                
                # Add title if specified
                if add_title:
                    # Create a title DataFrame
                    title_df = pd.DataFrame([[add_title]], columns=[''])
                    title_df.to_excel(writer, sheet_name=sheet_name, 
                                    startrow=0, startcol=0, index=False, header=False)
                    start_row = 2  # Leave a blank row after title
                
                # Write main data
                data.to_excel(writer, sheet_name=sheet_name,
                            startrow=start_row, index=include_index)
                
                # Get the workbook and worksheet for basic formatting
                workbook = writer.book
                worksheet = writer.sheets[sheet_name]
                
                # Basic formatting
                if add_title:
                    # Make title bold
                    title_cell = worksheet['A1']
                    title_cell.font = title_cell.font.copy(bold=True, size=14)
                
                # Auto-adjust column widths (simple approach)
                for column in worksheet.columns:
                    max_length = 0
                    column_letter = column[0].column_letter
                    for cell in column:
                        try:
                            if len(str(cell.value)) > max_length:
                                max_length = len(str(cell.value))
                        except:
                            pass
                    adjusted_width = min(max_length + 2, 50)  # Cap at 50
                    worksheet.column_dimensions[column_letter].width = adjusted_width
            
            logger.info(f"Excel file written successfully: {output_file}")
            
        except PermissionError as e:
            error_msg = f"Permission denied writing to {output_file}: {e}"
            logger.error(error_msg)
            result.add_error(error_msg)
            return result.error()
            
        except Exception as e:
            error_msg = f"Failed to write Excel file: {e}"
            logger.error(error_msg, exc_info=True)
            result.add_error(error_msg)
            return result.error()
        
        # Get file statistics
        try:
            file_stats = output_file.stat()
            file_size = file_stats.st_size
            logger.info(f"File size: {file_size} bytes")
        except Exception as e:
            logger.warning(f"Could not get file statistics: {e}")
            file_size = 0
        
        # Calculate metrics
        rows_written = len(data)
        columns_written = len(data.columns) if not data.empty else 0
        
        logger.info(f"Excel write completed", extra={
            "output_path": str(output_file),
            "rows_written": rows_written,
            "columns_written": columns_written,
            "file_size": file_size
        })
        
        # Add metrics
        result.add_metric("rows_written", rows_written)
        result.add_metric("columns_written", columns_written)
        result.add_metric("file_size_bytes", file_size)
        result.add_metric("sheet_name", sheet_name)
        result.add_metric("include_index", include_index)
        result.add_metric("has_title", add_title is not None)
        
        # Add artifacts
        result.add_artifact("output_path", str(output_file.resolve()))
        result.add_artifact("output_filename", output_file.name)
        
        # Add globals for downstream mods or reporting
        result.add_global("output_path", str(output_file.resolve()))
        result.add_global("rows_written", rows_written)
        result.add_global("file_size", file_size)
        
        return result.success()
        
    except KeyError as e:
        error_msg = f"Missing required parameter: {e}"
        logger.error(error_msg)
        result.add_error(error_msg)
        return result.error()
        
    except Exception as e:
        error_msg = f"Unexpected error in Excel writer: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result.add_error(error_msg)
        return result.error()
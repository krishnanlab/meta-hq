"""
Helper functions for CLI commands.

Author: Parker Hicks
Date: 2025-11-21

Last updated: 2025-11-21 by Parker Hicks
"""

import os
from pathlib import Path
import json

def set_verbosity(quiet: bool):
    """Return the opposite of quiet."""
    if quiet:
        return False
    return True

def dir_to_nested_dict(path):
    """Get the rough directory sturcture of database, edit output for _validate.py"""
    result = {}
    
    try:
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            
            if os.path.isdir(item_path):
                result[item] = dir_to_nested_dict(item_path)
            else:
                if '__files__' not in result:
                    result['__files__'] = []
                result['__files__'].append(item)
    except PermissionError:
        result['__error__'] = 'Permission denied'
    
    return print(json.dumps(result, indent=2))

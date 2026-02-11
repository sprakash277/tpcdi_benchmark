#!/usr/bin/env python3
"""Fix notebook_task structure in create_v2_workflow.py"""

import re

with open("create_v2_workflow.py", "r") as f:
    content = f.read()

# Fix missing commas after description
content = re.sub(r'"description":\s*"([^"]+)"\s*\n\s*"job_cluster_key"', r'"description": "\1",\n        "job_cluster_key"', content)
content = re.sub(r'"description":\s*"([^"]+)"\s*\n\s*"depends_on"', r'"description": "\1",\n        "depends_on"', content)

# Fix notebook_task structure - add notebook_path and convert parameters to base_parameters
def fix_notebook_task(match):
    task_content = match.group(0)
    
    # Extract notebook path from context (look for bronze/silver/gold paths)
    notebook_path = None
    if '/bronze/' in task_content:
        if '02_load_bronze_batch1' in task_content:
            notebook_path = 'f"{base_path}/bronze/02_load_bronze_batch1"'
        elif '03_load_bronze_incremental' in task_content:
            notebook_path = 'f"{base_path}/bronze/03_load_bronze_incremental"'
        elif 'bronze_create_' in task_content:
            notebook_path = 'notebook_path'  # Will be set in loop
    elif '/silver/' in task_content:
        if '02_transform_silver_batch1' in task_content:
            notebook_path = 'f"{base_path}/silver/02_transform_silver_batch1"'
        elif '03_transform_silver_incremental' in task_content:
            notebook_path = 'f"{base_path}/silver/03_transform_silver_incremental"'
        elif 'silver_create_' in task_content:
            notebook_path = 'notebook_path'  # Will be set in loop
    elif '/gold/' in task_content:
        if '02_load_gold_batch1' in task_content:
            notebook_path = 'f"{base_path}/gold/02_load_gold_batch1"'
        elif '03_load_gold_incremental' in task_content:
            notebook_path = 'f"{base_path}/gold/03_load_gold_incremental"'
        elif 'gold_create_' in task_content:
            notebook_path = 'notebook_path'  # Will be set in loop
    
    # Convert parameters array to base_parameters dict
    if '"parameters":' in task_content:
        # Extract parameters
        params_match = re.search(r'"parameters":\s*\[(.*?)\]', task_content, re.DOTALL)
        if params_match:
            params_str = params_match.group(1)
            # Extract key-value pairs
            param_pairs = re.findall(r'\{"key":\s*"([^"]+)",\s*"value":\s*"([^"]+)"\}', params_str)
            base_params = {}
            for key, value in param_pairs:
                # Remove var. prefix and convert to simple key
                clean_key = key.replace('var.', '')
                base_params[clean_key] = value
            
            # Build base_parameters dict string
            base_params_str = '{\n                '
            base_params_str += ',\n                '.join([f'"{k}": "{v}"' for k, v in base_params.items()])
            base_params_str += ',\n            }'
            
            # Replace
            if notebook_path and notebook_path != 'notebook_path':
                replacement = f'"notebook_task": {{\n            "notebook_path": {notebook_path},\n            "base_parameters": {base_params_str},\n            "source": "WORKSPACE"\n        }}'
            else:
                replacement = f'"notebook_task": {{\n            "notebook_path": notebook_path,\n            "base_parameters": {base_params_str},\n            "source": "WORKSPACE"\n        }}'
            
            return re.sub(r'"notebook_task":\s*\{[^}]+\}', replacement, task_content, flags=re.DOTALL)
    
    return task_content

# Apply fixes for notebook_task blocks
content = re.sub(r'"notebook_task":\s*\{[^}]+\}', fix_notebook_task, content, flags=re.DOTALL)

# Fix table creation tasks - add notebook_path conversion
content = re.sub(
    r'(task_key = f"[^"]+_create_([^"]+)"\s+.*?# Convert SQL file path to notebook path\s+notebook_path = )sql_file_path\.replace\("/tables/create_", "/notebooks/create_"\)\.replace\("\.sql", ""\)',
    r'\1sql_file_path.replace("/tables/create_", "/notebooks/create_").replace(".sql", "")',
    content,
    flags=re.DOTALL
)

# Fix missing notebook_path in table creation tasks
content = re.sub(
    r'"notebook_task":\s*\{\s*"parameters":',
    r'"notebook_task": {\n                "notebook_path": notebook_path,\n                "base_parameters":',
    content
)

# Fix parameters to base_parameters in table creation
content = re.sub(
    r'"base_parameters":\s*\{[^}]*"parameters":\s*\[(.*?)\][^}]*\}',
    lambda m: fix_params(m.group(1)),
    content,
    flags=re.DOTALL
)

def fix_params(params_str):
    param_pairs = re.findall(r'\{"key":\s*"([^"]+)",\s*"value":\s*"([^"]+)"\}', params_str)
    base_params = {}
    for key, value in param_pairs:
        clean_key = key.replace('var.', '')
        base_params[clean_key] = value
    return '{\n                ' + ',\n                '.join([f'"{k}": "{v}"' for k, v in base_params.items()]) + ',\n            }'

with open("create_v2_workflow.py", "w") as f:
    f.write(content)

print("Fixed notebook_task structures")

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan  7 14:53:39 2025

@author: gergo.havasi
"""

import pandas as pd
import re

# Load the Excel file containing stored procedures
excel_file_path = 'MarketSurge Reports Data Items (1).xlsx'
df = pd.read_excel(excel_file_path)
stored_procedure_names = df.iloc[:, 0].tolist()
stored_procedure_code = df.iloc[:, 1].fillna('').astype(str).tolist()

# Define SQL clauses to track
sql_clauses = ['SELECT', 'INSERT', 'UPDATE', 'WHERE', 'GROUP BY', 'ORDER BY', 'HAVING']

# Define a regex pattern to capture tables and fields
field_pattern = re.compile(r'(\w+)\.(\w+)')

# Initialize a results list
results = []

# Iterate through each stored procedure
for sproc in stored_procedure_code:
    sproc_results = {'Stored Procedure': sproc}
    for clause in sql_clauses:
        sproc_results[clause] = []

    # Split by SQL clauses for parsing
    for clause in sql_clauses:
        clause_start = re.split(f'{clause}', sproc, flags=re.IGNORECASE)
        if len(clause_start) > 1:
            # Capture everything after the clause till the next one or end
            next_clauses = '|'.join(sql_clauses)
            clause_content = re.split(next_clauses, clause_start[1], flags=re.IGNORECASE)[0]
            fields = field_pattern.findall(clause_content)
            for table, field in fields:
                field=field.lower()
                sproc_results[clause].append(f'{field}')

    results.append(sproc_results)

# Convert results into a DataFrame
final_results = []
counter=0
for res in results:
    
    
    for clause in sql_clauses:
        for field in res[clause]:
            final_results.append({
                'Stored Procedure name': stored_procedure_names[counter],
                'Stored Procedure code': res['Stored Procedure'],
                'Table.Field': field,
                'Clause': clause
            })
    counter=counter +1
# Create a DataFrame and export to Excel
output_df = pd.DataFrame(final_results)
output_file_path = 'sproc_analysis_results.xlsx'
output_df.to_excel(output_file_path, index=False)

print(f"Analysis complete! Results saved to {output_file_path}")

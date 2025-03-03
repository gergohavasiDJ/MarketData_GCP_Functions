# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
import sqlparse
import re

# Load fields from Excel
excel_file = '/Users/gergo.havasi/Library/CloudStorage/GoogleDrive-gergo.havasi@dowjones.com/My Drive/Python projects/Fileds not used in SPROCs/MarketSurge Reports Data Dictionary.xlsx'

fields_df = pd.read_excel(excel_file)


fields_df = fields_df.loc[fields_df['SPROC']=='pr_MS_DG_GetEPSTopRatingCompanies']
fields_list = fields_df['Data Item'].tolist()
# Example SPROC SQL (replace with the actual SPROC content)
sproc_sql = """
    CREATE PROCEDURE [dbo].[pr_MS_DG_GetEPSTopRatingCompanies]  
AS  
  
SET NOCOUNT ON  
  
DECLARE @top INT  
    SET @top = 150  
  
 SELECT TOP (@top) Symbol, CoName, Exchange, Price, [Price Change],  
   VolPctChg, AVol, Eps, Rlst, Smrl, AccDisRtg, Comp  
   FROM (  
     SELECT TOP 100 G.symbol,   
       CASE WHEN (g.osid in(select S.osid from dgRPTSnapshot S where s.dgRPT='DG01')) THEN ' ' + G.coname ELSE '*' + G.coname END AS 'coname',       --added to reflect diff in snapshot  
       CASE WHEN g.exchcd=1 THEN 'NYSE' ELSE 'AMEX' END AS 'Exchange',  
       g.price0 as 'Price',  
       G.price0 - G.price  AS 'Price Change' ,    
       A.volPctChg,  
       A.avol,  
       CASE WHEN G.epsrnk=0 THEN 'na' ELSE cast(G.epsrnk as varchar(3)) END AS 'eps',   
       G.rlst,   
       CASE WHEN G.smrl='' THEN 'N/A' ELSE G.smrl END AS 'smrl',   
       CASE WHEN G.accDis='' OR G.accDis IS NULL THEN 'N/A' ELSE G.accdis END AS 'AccDisRtg',   
       CASE WHEN M.smartSelect is NULL THEN 'na' ELSE cast(M.smartSelect as varchar(3)) END AS 'comp'  
       FROM  getrsm1 G   
       --INNER JOIN dlgBook DB ON G.osid = DB.osid  
       INNER JOIN cs_AveVolumesView av ON G.OSID = av.osid  
       INNER JOIN avolView A ON G.osid = A.osid  
       INNER JOIN mainFrameStockRatings M ON G.osid=M.osid, theTime W       
      WHERE (G.exchcd = 1 OR G.exchcd=2)   
      AND G.rlst > 75   
      AND g.Price0 >= 5      
      AND g.Price0 * g.CAPTL >= 100000      
      AND av.avdolv >= 500000      
        --AND DB .bluBK = 1       
      ORDER BY G.epsrnk DESC, G.rlst DESC, coName       
      UNION  
     SELECT TOP 100 G.symbol,   
       CASE WHEN (g.osid in(SELECT S.osid FROM dgRPTSnapshot S where s.dgRPT='DG02')) THEN ' ' + G.coname ELSE '*' + G.coname END AS 'coname',      --added 04/17/01 to reflect stars  
       'NASDAQ' AS 'Exchange',  
       G.price0 AS 'Price',   
       (G.price0 - G.price) AS 'Price Change',    
       A.volPctChg,  
       A.avol,  
       CASE WHEN G.epsrnk=0 THEN 'na' ELSE cast(G.epsrnk as varchar(3)) END AS 'eps',   
       G.rlst,   
       CASE WHEN G.smrl='' THEN 'N/A' ELSE G.smrl END AS 'smrl',   
       CASE WHEN G.accDis=''  OR G.accDis IS NULL THEN 'N/A' ELSE G.accdis END AS 'AccDisRtg',   
       CASE WHEN M.smartSelect is NULL THEN 'na' ELSE cast(M.smartSelect as varchar(3)) END AS 'comp'  
       FROM getrsm1 G   
       INNER JOIN avolView A ON G.osid = A.osid  
       INNER JOIN cs_AveVolumesView av ON G.OSID = av.osid  
       --INNER JOIN dlgBook D ON G.osid=D.osid  
       INNER JOIN mainFrameStockRatings M ON G.osid=M.osid, thetime W  
      WHERE G.exchcd >= 80    
      AND G.rlst > 75   
      --AND D .grnBk = 1  
      AND g.Price0 >= 5      
      AND g.Price0 * g.CAPTL >= 100000      
      AND av.avdolv >= 500000   
      ORDER BY G.epsrnk DESC, G.rlst DESC, coName       
   )x  
  ORDER BY eps DESC, rlst DESC, coName  
  
SET NOCOUNT OFF 
"""

# Helper functions
def extract_insert_into_fields(sql_query):
    # Match fields in the INSERT INTO statement
    matches = re.findall(r'INSERT INTO\s+[^\(]+\((.*?)\)\s+SELECT', sql_query, re.DOTALL | re.IGNORECASE)
    fields = set()
    for match in matches:
        fields.update(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', match))
    return fields

def extract_select_fields_from_insert(sql_query):
    # Match fields in the SELECT clause within an INSERT INTO
    matches = re.findall(r'INSERT INTO\s+[^\(]+\([^\)]+\)\s+SELECT\s+(.*?)(?:\s+FROM|\s*$)', sql_query, re.DOTALL | re.IGNORECASE)
    fields = set()
    for match in matches:
        fields.update(re.findall(r'\b[a-zA-Z_][a-zA-Z0-9_]*\b', match))
    return fields

# Extract fields from INSERT INTO and SELECT clauses
insert_into_fields = extract_insert_into_fields(sproc_sql)
select_fields_for_insert = extract_select_fields_from_insert(sproc_sql)

# Determine field usage
fields_df['Used'] = fields_df['Data Item'].apply(
    lambda x: 'Used' if x in insert_into_fields or x in select_fields_for_insert else 'Not Used'
)

# Save to Excel for review
fields_df.to_excel('fields_usage_analysis.xlsx', index=False)

print("Fields identified as used in INSERT INTO or SELECT clauses have been flagged.")
print(f"Analysis saved to 'fields_usage_analysis.xlsx'")
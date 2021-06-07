import pandas as pd
import numpy as np
import json

def ddl_generation(total_csv):
    l=total_csv['TARGET_TABLE_NAME'].nunique()
    listofuniquetargets = pd.unique(total_csv['TARGET_TABLE_NAME']).tolist()
    for i in range(l):
        target0 = total_csv.loc[total_csv['TARGET_TABLE_NAME'] == listofuniquetargets[i]]
        t=target0['TARGET_TABLE_NAME'].iloc[0]
        v=target0['TARGET_DATASET_NAME'].iloc[0]
        str(v).strip(" ")
        a=target0['TARGET_COLUMN_NAME'].tolist()
        b=target0['TARGET_DATA_TYPE'].tolist()
        d=target0['TARGET_PARTITION'].tolist()
        e=target0['DESCRIPTION'].tolist()
        pop=[]
        f = open(r'out/bq_DDL.txt', 'a')
        f.write('CREATE OR REPLACE TABLE `bt-uk-nucl-tst-live.'+str.lower(v)+'.'+str.lower(t)+'`'+'(')
        for n in range(0, len(a)):
            if(target0['NULLABLE'].iloc[n]=='REQUIRED'):
                if pd.isnull(e[n]):
                    c= a[n]+'   '+b[n] + ' NOT NULL'
                    if(n==0):
                        f.write(c)
                    else:
                        f.write(', '+c)
                else:
                    x = e[n].split('\\')
                    temp = "'".join(x)
                    e[n] = temp
                    x = e[n].split('"')
                    temp = "'".join(x)
                    e[n]=temp
                    c= a[n]+'   '+b[n]+' '+'NOT NULL'+' options(description= "'+str(e[n])+'")'
                    if(n==0):
                        f.write(c)
                    else:
                        f.write(', '+c)
            else:
                if pd.isnull(e[n]):
                    c= a[n]+'   '+b[n]
                    if(n==0):
                        f.write(c)
                    else:
                        f.write(', '+c)
                else:
                    x = e[n].split('\\')
                    temp = "'".join(x)
                    e[n] = temp
                    x = e[n].split('"')
                    temp = "'".join(x)
                    e[n]=temp
                    c= a[n]+'   '+b[n]+' '+'options(description= "'+str(e[n])+'")'
                    if(n==0):
                        f.write(c)
                    else:
                        f.write(', '+c)
        if('Y' not in d):
            par = ''
            f.write(')'+par+' Options (description= "data ingested into '+str(t)+'");'+'\n')
        else:
            partition_key_data_type = target0.loc[target0['TARGET_PARTITION']=='Y','TARGET_DATA_TYPE'].iat[0]
            partition_key = target0.loc[target0['TARGET_PARTITION']=='Y','TARGET_COLUMN_NAME'].iat[0]
            par = str(partition_key)
            if(partition_key=='ACTIVE_FL'):
                par = ')partition by END_OF_VALIDITY cluster by ACTIVE_FL'
                f.write(par+' Options (Description= "data ingested into '+str(t)+'");'+'\n')
            elif (partition_key_data_type=='TIMESTAMP' or partition_key_data_type=='DATETIME'):
                par = ')partition by DATE('+par+')'
                f.write(par+ ' Options (Description= "data ingested into '+str(t)+'");'+'\n')
            else:
                par = ')partition by '+par
                f.write(par+' Options (Description= "data ingested into '+str(t)+'");'+'\n');
        f.close()
if(__name__=='__main__'):
    #total_csv = pd.read_excel(r"", sheet_name=0, header=None, skiprows=0, na_values=' ')
    total_csv = pd.read_csv(r"input/Mapping_Sheet.csv",encoding='utf8', sep=',', header=None, skiprows=0, na_values=' ', error_bad_lines=False)
    total_csv.columns = total_csv.iloc[0]
    total_csv = total_csv[1:]
    total_csv.replace('', np.nan, inplace=True)
    total_csv['NULLABLE'] = total_csv['NULLABLE'].replace(['N'],'REQUIRED')
    total_csv['NULLABLE'] = total_csv['NULLABLE'].replace(['Y'],'NULLABLE')
    total_csv['TARGET_DATA_TYPE'] = total_csv['TARGET_DATA_TYPE']
    ddl_generation(total_csv)
    #recipe_generation(total_csv)
    #schema_generation(total_csv)

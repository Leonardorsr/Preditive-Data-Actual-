import pandas  as pd
from datetime import date
import pandas_gbq

#Lendo o database do github
dfgit=pd.read_csv('https://raw.githubusercontent.com/Leonardorsr/Preditive-Data-Actual-/main/Bae_dados_gasto.csv',index_col="DATE")


#Coletando o dia atual e preparando as variáveis
today = date.today()
today=str(today)
new_data=dfgit[:today]

values_list=[]
date_list=[]

#Coleta dos valores do github
for n in new_data.values:
  values_list.append(n[0])
for d in new_data.index:
  date_list.append(d)
  
#Criação do novo dataframe com a devida formatação
dfnew = pd.DataFrame({"DATE":date_list,"VALUES":values_list})

#Atualização do datalake no GCP
pandas_gbq.to_gbq(dfnew,"pure-century-423811-n3.Base_de_dados.Energy_Spent",#projeto.dataset.tabela,
                      project_id='pure-century-423811-n3',
                      reauth=False, if_exists="replace", api_method="load_csv")
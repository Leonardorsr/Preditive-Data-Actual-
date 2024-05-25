import numpy as np
import pandas_gbq
import pandas as pd, datetime
from statsmodels.tsa.stattools import adfuller
import matplotlib.pyplot as plt
from time import time
import os
from math import sqrt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import itertools
import statsmodels
import statsmodels.api as sm
from statsmodels.tsa.stattools import acf,pacf
from statsmodels.tsa.arima_model import ARIMA
from sklearn import model_selection
from sklearn.metrics import mean_squared_error, r2_score
import warnings
from google.oauth2 import service_account
warnings.filterwarnings('ignore')

sql = """SELECT * FROM `pure-century-423811-n3.Base_de_dados.Energy_Spent`
ORDER BY DATE"""

project_id = "pure-century-423811-n3"
df = pd.read_gbq(sql, project_id=project_id, dialect="standard")
#print(df)

df['DATE'] = pd.to_datetime(df['DATE'])
df['VALUE']= df['VALUE']*1.0
timeseries= df.set_index('DATE')['VALUE'].resample('W').sum()
df_teste = timeseries['2023':]
timeseries_teste= df.set_index('DATE')['VALUE'].resample('W').sum()


# Visualizando os dados
roll_mean = timeseries.rolling(window=7).mean()
roll_std = timeseries.rolling(window=7).std()

# Plotting rolling statistics:
orig = plt.plot(timeseries.resample('W').mean(), color='blue',label='Original')
mean = plt.plot(roll_mean.resample('W').mean(), color='red', label='Rolling Mean')
std = plt.plot(roll_std.resample('W').mean(), color='green', label = 'Rolling Std')
plt.legend(loc='best')
plt.show(block=False)

# Fazendo o teste Dickey-Fuller:
print('Results of Dickey-Fuller Test:')
result = adfuller(timeseries, autolag='AIC')
print('ADF Statistic: %f' % result[0])
print('p-value: %f' % result[1])
print('Critical Values:')
for key, value in result[4].items():
    print(key, value)
    
    
#Criando uma lista com todas as combinações possíveis dos parâmetros p,d,q
p = d = q = range(0, 3)
pdq = list(itertools.product(p, d, q))
seasonal_pdq = [(x[0], x[1], x[2], 12) for x in list(itertools.product(p, d, q))]

#Testando cada combinação com o AIC para achar o melhor valor
train_arima = timeseries
train_arima = train_arima.astype(float)
print(train_arima)
least_AIC = 999999
for param in pdq:
    for param_seasonal in seasonal_pdq:
        mod = sm.tsa.statespace.SARIMAX(train_arima,
                                            order=param,
                                            seasonal_order=param_seasonal,
                                            enforce_stationarity=False,
                                            enforce_invertibility=False)
        results = mod.fit(disp=0)
        #print('ARIMA{}x{}12 - AIC:{}'.format(param, param_seasonal, results.aic))
        if least_AIC > results.aic and results.aic > 100:
            least_AIC = results.aic
            best_param = param
            best_seasonal_param = param_seasonal
            best_aic = results.aic
print('Os são melhores parâmetros são: ',best_param, best_seasonal_param,best_aic)



#Criando o modelo ARIMA
best_seasonal_param=(2,2,1,12)
best_param=(2,1,1)
model = sm.tsa.statespace.SARIMAX (train_arima,
                                    order=best_param,
                                    seasonal_order=best_seasonal_param,
                                    enforce_stationarity=False,
                                    enforce_invertibility=False)
model_fit = model.fit()
print(model_fit.summary().tables[1])
model_fit.plot_diagnostics(figsize=(10, 10))
plt.show()



#Fazendo a previsão
#print(model_fit)
pred = model_fit.get_prediction(start=pd.to_datetime('2024-01-07'), end = pd.to_datetime('2024-12-31') ,dynamic = False)
pred_rmse = model_fit.get_prediction(start=pd.to_datetime('2024-01-07') ,dynamic = False)

# Plotting e calculando o RMSE
pred_ci = pred.conf_int()
timeseries_teste = timeseries_teste.astype(float)
ax = timeseries_teste["2023":].plot(label = "observed", figsize=(15, 7))
pred.predicted_mean.plot(ax = ax, label = "One-step ahead Forecast", alpha = 1)
ax.fill_between(pred_ci.index,
                pred_ci.iloc[:, 0],
                pred_ci.iloc[:, 1],
                color = "k", alpha = 0.05)
ax.set_xlabel("DATA")
ax.set_ylabel("VALUE")
plt.legend
plt.show()
train_arima_forecasted = pred_rmse.predicted_mean
timeseries_teste_truth = timeseries_teste['2024-01-01':]
# Calculating the error
rms_arima = sqrt(mean_squared_error(timeseries_teste_truth, train_arima_forecasted))
print("Root Mean Squared Error: ", rms_arima)


#Devolvendo para a nuvem
predictions=pred.predicted_mean
dfpredicted = pd.DataFrame({'Date':predictions.index,'Predicted':predictions.values}  )
pandas_gbq.to_gbq(dfpredicted,"pure-century-423811-n3.Base_de_dados.Energy_Predicted",#projeto.dataset.tabela,
                      project_id='pure-century-423811-n3',
                      reauth=False, if_exists="replace", api_method="load_csv")
dfpredicted.to_excel('Base_dados_gasto.xlsx')
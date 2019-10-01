import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date
import dateutil.parser
from pandas import DataFrame

import warnings
warnings.filterwarnings('ignore')

import time

import dask.dataframe as dd
from dask import delayed

import os



# Funcion que genera un DataFrame con todos los dias (sin repetir) para los cuales se tienen datos

def GetDate(stockdata):
    '''
    Parameters:
    ------
    stockdata:
    DataFrame - Data of various stocks

    Return:
    ------
    days: DataFrame - DataFrame of all days for which there is data
    '''

    days = pd.DatetimeIndex(stockdata.index).normalize()
    days = pd.DataFrame(days)
    days.index = stockdata.index
    days.columns = ['dia']

    return days.drop_duplicates(keep='first').dia





def StockPreprocessing(stockdata, stock_ticker):
    """
    Parameters:
    ------
    stockdata:
    DataFrame - Data of various stocks

    stock_tickers:
    List, string - Stock ticker strings
    """

    ##############################################################################3

    # Funcion que inicializa las columnas: 'nombre', 'date_time', 'tipo', 'precio', 'volumen',
    #                                      'BID', 'ASK', 'Mid_Price', 'Quoted_Spread'

    def stock_preprocessing(stockdata, stock_ticker):
        '''
        Parameters:
        ------
        stockdata:
        DataFrame - Data of various stocks

        stock_ticker:
        String - Ticker of the single stock we are interested in


        Return:
        ------
        stockdata:
        DataFrame - Data of stocks with the folloeing initialized columns:
        nombre', 'date_time', 'tipo', 'precio', 'volumen', 'BID', 'ASK', 'Mid_Price', 'Quoted_Spread'
        '''

        stockname = stock_ticker + " CB Equity"

        # Se cambian los nombres de las columnas y se elimina lo demas
        #stockdata         = stockdata[['name', 'times', 'type', 'value', 'size']] # ASI ERA ORIGINALMENTE, SE CAMBIO PARA LA PRESENTACION
	stockdata         = stockdata[['ticker', 'timestamp', 'type', 'price', 'volume']]
        stockdata.columns = ['nombre','date_time','tipo','precio','volumen']
        stockdata         = stockdata[['nombre','date_time','tipo','precio','volumen']]


        # Seleccionamos los datos segun la accion y el horario que nos interesan
        stockdata        = stockdata.loc[(stockdata["nombre"] == stockname)]
        stockdata.index  = stockdata.date_time
        stockdata        = stockdata.between_time('9:30','15:55')
        stockdata['dia'] = pd.DatetimeIndex(stockdata.date_time).normalize()

        # DataFrame de los dias para los cuales stockdata tiene datos
        days = GetDate(stockdata)

        # Lista donde se guardaran los resultados de los calculos diarios
        BA = []

        for i in days:

            # Sacamos los datos para un unico dia
            stockdailydata = stockdata[stockdata.dia == str(i)]

            # Inicializamos columnas de precios BID y ASK
            init_values = stockdailydata.precio.values
            d           = {'BID': init_values, 'ASK': init_values}
            BA_df       = pd.DataFrame(data=d)

            # Calculamos los valores de los precios BID y ASK
            bid       = stockdailydata['tipo'] == 'BID'
            ask       = stockdailydata['tipo'] == 'ASK'
            BA_df.BID = np.multiply(bid.values, stockdailydata.precio.values)
            BA_df.ASK = np.multiply(ask.values, stockdailydata.precio.values)

            # Para los precios iguales a cero se arrastra el valor anterior
            BA_df['BID'] = BA_df['BID'].replace(to_replace = 0, method = 'ffill').values
            BA_df['ASK'] = BA_df['ASK'].replace(to_replace = 0, method = 'ffill').values

            # Donde el BID sea menor o igual al precio ASK se pone un NaN
            BA_df = BA_df.where(BA_df.BID <= BA_df.ASK, np.nan)

            # Para los precios del principio del dia, debemos poner NaN
            cols = ['BID', 'ASK']
            BA_df[cols] = BA_df[cols].replace({0:np.nan, 0:np.nan})

            # Con los precios BID y ASK se calcula el Mid_Price y el Quoted_Spread
            BA_df['Mid_price']     = 0.5*(BA_df['BID'].values + BA_df['ASK'].values)
            BA_df['Quoted_Spread'] = (BA_df['ASK'].values - BA_df['BID'].values)/(BA_df.Mid_price.values)

            # Se guarda todo en el DataFrame BA
            BA.append(BA_df)

        # Se unifican los valores para todos los dias
        BA        = pd.concat(BA, axis=0)
        BA.index  = stockdata.index
        stockdata = pd.concat([stockdata, BA], axis=1)

        # Reordenamos las columnas
        stockdata = stockdata[["date_time", "dia", "nombre", "tipo", "precio", "volumen", "ASK", "BID",
                               "Mid_price", "Quoted_Spread"]]
        stockdata = stockdata.drop(columns=["date_time"])

        return stockdata

    ##############################################################################3

    if (type(stock_ticker) == list):
        # Temporary list where we will save results
        temp = []

        # We apply the preprocessing procedure to our list of stocks
        for ticker in stock_ticker:

            # We preprocess the data individually by ticker
            temp.append(stock_preprocessing(stockdata, ticker))

        # Concatenate the results
        data = pd.concat(temp, sort=True)

        # Return a dataframe sorted by timestamp
        data = data.sort_index()
        data = data[["dia", "nombre", "tipo", "precio", "volumen", "ASK", "BID",
                     "Mid_price", "Quoted_Spread"]]
        return data

    else:
        return stock_preprocessing(stockdata, stock_ticker)





def sep_date(stockdata):
    '''
    Parameters:
    ------
    stockdata:
    DataFrame - Data of a single stock

    Return:
    ------
    daily_dfs:
    List - A list whose entries are DataFrames of data in a particular day
    '''

    # Obtenemos los dias para los cuales tenemos datos
    days = GetDate(stockdata)

    # Creamos la lista donde guardaremos los datos correspondientes a un dia
    daily_dfs = []

    # Separamos stockdata por dias
    for i in days:
        daily_dfs.append(stockdata.loc[stockdata["dia"] == i])

    return daily_dfs





def StockDepth(stockdata, stock_ticker):
    '''
    Parameters:
    ------
    stockdata:
    DataFrame - Preprocessed data of a several stocks

    stock_ticker:
    List - List of stock ticker strings

    Return:
    ------
    result:
    DataFrame - Results of the depth calculations
    '''

    ##############################################################################3

    def stock_depth(stockdata, stock_ticker):

        # Seleccionamos los datos de la accion que nos interesa
        stockname = stock_ticker + " CB Equity"
        stockdata = stockdata.loc[stockdata["nombre"]==stockname]

        # Obtenemos un arreglo de los dias para los cuales tenemos datos
        days = stockdata["dia"].drop_duplicates().values

        # Inicializamos la lista en donde guardaremos los resultados intermedios
        temp = []

        # Calculamos las profundidades diarias
        for day in days:

            # Seleccionamos los datos del dia que estamos evaluando
            df = stockdata.loc[stockdata["dia"]==day][["tipo", "precio", "volumen"]].copy()

            # Primeras condiciones - Ponemos el volumen de la orden segun su tipo
            a = np.where(df['tipo'] == 'ASK', df.volumen, 0)
            b = np.where(df['tipo'] == 'BID', df.volumen, 0)

            # Condicion - transacciones tipo TRADE de venta
            a_t = (np.where(df['tipo'] == 'TRADE',
                            (np.where(df['tipo'].shift(1) == 'ASK',
                                      (np.where(df['precio'] == df['precio'].shift(1), -df.volumen, 0)), 0)), 0))

            # Condicion - transacciones tipo TRADE de compra
            b_t = (np.where(df['tipo'] == 'TRADE',
                            (np.where(df['tipo'].shift(1) == 'BID',
                                      (np.where(df['precio'] == df['precio'].shift(1), -df.volumen, 0)), 0)), 0))

            # Realizamos la suma acumulada de las profundidades segun las condiciones
            df['ASK_depth'] = (np.where(a_t != 0, a_t, a)).cumsum()
            df['BID_depth'] = (np.where(b_t != 0, b_t, b)).cumsum()
            df['Depth']     = df['ASK_depth'] + df['BID_depth']
            df['log_depth'] = np.where(df['Depth'].values == 0, np.nan, np.log(df['Depth'].values))

            # Eliminamos columnas redundantes
            df = df.drop(columns=["tipo", "precio", "volumen"])

            # Vamos guardando los resultados de cada dia
            temp.append(df)

        # Concatenamos los resultados obtenidos en un unico DataFrame
        temp = pd.concat(temp, sort=True)

        # Concatenamos los resultados obtenidos con los datos previos
        result = pd.concat([stockdata, temp], axis=1)

        # date_time es el indice del DataFrame, entonces tenerlo como otra columna es redundante
        #result = result.drop(columns=["date_time"])

        # Reordenamos las columnas
        result = result[["dia", "nombre", "tipo", "precio", "ASK", "BID", "Mid_price", "Quoted_Spread", "volumen",
                         "ASK_depth", "BID_depth", "Depth", "log_depth"]]
        # Retornamos el resultado
        return result

    ##############################################################################3

    if type(stock_ticker) == list:

        # A list where we will store intermediate results is initialized
        temp = []

        # We calculate each stock's depth individually
        for ticker in stock_ticker:

            # A stock is selected
            stockname = ticker + " CB Equity"
            df = stockdata.loc[stockdata["nombre"]==stockname]

            # Stock results are appended
            temp.append(stock_depth(df, ticker))

        # Results are concatenated
        result = pd.concat(temp, sort=True)

        # Reordering the columns
        result = result[["dia", "nombre", "tipo", "precio", "ASK", "BID", "Mid_price", "Quoted_Spread", "volumen",
                         "ASK_depth", "BID_depth", "Depth", "log_depth"]]

        return result.sort_index()

    else:

        return stock_depth(stockdata, stock_ticker)





def InitiatingParty(stockdata):
    '''
    Parameters:
    ------
    stockdata:
    DataFrame - Preprocessed data of the stock

    Return:
    ------
    x:
    DataFrame - DataFrame of TRADE quotes with the party that initiated the trade
    '''

    # Only TRADE operations are considered
    stockdata = stockdata.loc[stockdata["tipo"] == "TRADE"]

    # Values are filled according to the transaction's direction criteria
    stockdata["iniciado"] = np.where(stockdata.precio > stockdata.Mid_price, 1,
                                     np.where(np.isnan(stockdata.Mid_price), np.nan, -1))

    return stockdata





from datetime import timedelta
from sklearn import linear_model as lm
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression





def ImpactParameters(stockdata, stock_ticker):

    ##############################################################################3

    def impact_params(stockdata):
        days = stockdata["dia"].drop_duplicates().values
        res  = []

        for day in days:
            stockdailydata = stockdata.loc[stockdata["dia"] == day]

            stockdailydata['delta_p']    = stockdailydata['precio'].diff()
            stockdailydata['order_flow'] = stockdailydata.volumen.values * stockdailydata.iniciado.values

            res.append(stockdailydata)

        res_df = pd.concat(res, axis=0)
        return res_df

    ##############################################################################3

    if(type(stock_ticker) == list):

        temp = []

        for ticker in stock_ticker:

            stockname = ticker + " CB Equity"
            df = stockdata.loc[stockdata["nombre"]==stockname]
            temp.append(impact_params(df))

        result = pd.concat(temp, sort=True)

        return result.sort_index()

    else:
        stockname = stock_ticker + " CB Equity"
        df = stockdata.loc[stockdata["nombre"]==stockname]

        return impact_params(df)





def KyleImpactRegression(stockdata, stock_ticker):

    ##############################################################################3

    def kyle(stockdata):

        #days = GetDate(stockdata)#.drop_duplicates(keep='first').dia
        days = stockdata["dia"].drop_duplicates().values
        res = []

        for i in days:

            stockdailydata = stockdata[stockdata.dia == str(i)]

            x1 = stockdailydata.delta_p.values
            x1 = x1.reshape(-1, 1)

            x2 = stockdailydata.order_flow.values
            x2 = sm.add_constant(x2.reshape(-1, 1))

            result = sm.OLS(x1, x2, missing='drop').fit()

            coef = result.params[1]
            pvalue = result.pvalues[1]
            trades = len(stockdailydata)

            temp = [i, coef, pvalue, trades]
            res.append(temp)

        res = pd.DataFrame(res, columns=['dia', 'coef_regresion', 'p_value', 'trades'])
        res = res.set_index('dia')

        return res

    ##############################################################################3

    if(type(stock_ticker)==list):

        result = dict()

        for ticker in stock_ticker:

            stockname = ticker + " CB Equity"
            df        = stockdata.loc[stockdata["nombre"]==stockname]

            result[ticker] = kyle(df)

        return result

    else:

        stockname = stock_ticker + " CB Equity"
        stockdata = stockdata.loc[stockdata["nombre"]==stockname]

        return kyle(stockdata)

# Exploring Intraday data with Jupyter notebooks




## 1. Use Kaggle to download the sample dataset

We provide a sample dataset with intrady information for five stocks (PFBCOLO, ECOPETL, BCOLO, PFAVAL, ISA) that trade in the Colombian Stock Exchange (Bolsa de Valores de Colombia). Trade and Quote data provides information on the best bid and ask, trades on prices and volumes for stocks. The sample contains intraday information from April to August 2017.

You can download the data to you local computer from the [Kaggle repo](https://www.kaggle.com/ccastroiragorri/taqcolombiasample) or you can use the [Kaggle API](https://www.kaggle.com/docs/api)

In order to use the API first intall `kaggle` CLI tool 

```
pip install kaggle
```

You must open a kaggle account to get an API token `kaggle.json`. That should be stored  at ~/.kaggle/kaggle.json on Linux, OSX, and other UNIX-based operating systems, and at C:\Users<Windows-username>.kaggle\kaggle.json on Windows. 

Then in the command line navegate do where you want to download the data (this repo if you cloned it) and use the following command
```
kaggle datasets download -d ccastroiragorri/taqcolombiasample 
```

## 2. Use the Example jupyter notebook


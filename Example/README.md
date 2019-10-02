# Exploring Intraday data with Jupyter notebooks

## 1. Set up the enviorment

Create a new enviorment using the virtual enviorment package. Install if needed.

```
pip install virtualenv
```
Form the workspace directory create the virtual enviorment
```
python -m venv taq
```
Navigate though the folder that has been created
```
cd taq/Scripts
```
Activate virtual enviorment
```
activate
```
Navigate to the folder containing the `requirements.txt`
```
pip install -r requirements.txt
```

## 2. Use Kaggle to download the sample dataset

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

## Start jupyter notebook

Navigate to Example folder and follow th instructions in notebook `Example.ipynb`












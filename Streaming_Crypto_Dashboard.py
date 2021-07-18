import requests
import pandas as pd
from sqlalchemy import create_engine
import datetime
import urllib.request as urllib2
from datetime import datetime

# Credentials to database connection
hostname="localhost"
dbname="XXXDATABASE_NAMEXXX"
uname="XXUSERNAMEXX"
pwd="XXPASSWORDXX"


pd.set_option('display.max_rows', 120)

coins='BTC,BNB,ETH,ADA,XRP,DOT,LTC,BUSD,BCH,HT,EOS,LINK,TRX,DASH,DOGE,OKB,XLM,RVN,ETC,UNI,NEO,USDT,ZEC,BSV,REN,SXP,ATOM,BTT,SUSHI,CAKE,AAVE,XEM,MIOTA,IOST,XTZ,USDC,XMR,ONT,VET,FIL,MATIC,FTT,OMG,ICX,ALGO,XVS,YFI,QTUM,BAKE,CRV,SOL,AVAX,BNT,CRO,ZIL,GLM,KSM,LUNA,EGLD,SRM,SNX,THETA,JST,BAND,CHZ,TMTG,TFUEL,ZRX,CVC,WAVES,GRT,MANA,BAT,HBAR,FTM,GT,ELF,COMP,WBTC,CELO,NPXS,DGB,YFII,PAY,ALPHA,FRONT,DVP,RUNE,OXT,RSR,KNC,LSK,MX,NANO,REP,COTI,ENJ,LOOM,LRC,KAVA'
multi='https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD,EUR'
api='XXXAPI_KEY_HEREXXX'
extraParams='dogedash'
params=(
    f'extraParams={extraParams}&'
    f'fsyms={coins}&'
    f'api_key={api}')
url=multi+params

def GetCoinList(lst):
    global coins,CoinTable
    CoinTable={}
    coins='BTC,BNB,ETH,ADA,XRP,DOT,LTC,BUSD,BCH,HT,EOS,LINK,TRX,DASH,DOGE,OKB,XLM,RVN,ETC,UNI,NEO,USDT,ZEC,BSV,REN,SXP,ATOM,BTT,SUSHI,CAKE,AAVE,XEM,MIOTA,IOST,XTZ,USDC,XMR,ONT,VET,FIL,MATIC,FTT,OMG,ICX,ALGO,XVS,YFI,QTUM,BAKE,CRV,SOL,AVAX,BNT,CRO,ZIL,GLM,KSM,LUNA,EGLD,SRM,SNX,THETA,JST,BAND,CHZ,TMTG,TFUEL,ZRX,CVC,WAVES,GRT,MANA,BAT,HBAR,FTM,GT,ELF,COMP,WBTC,CELO,NPXS,DGB,YFII,PAY,ALPHA,FRONT,DVP,RUNE,OXT,RSR,KNC,LSK,MX,NANO,REP,COTI,ENJ,LOOM,LRC,KAVA'
    multi='https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD,EUR'
    api='66af8c1761ed78335ed4f5350cd6c7e1399700d224198dd620e77e82669c2295'
    extraParams='dogedash'
    params=(
        f'extraParams={extraParams}&'
        f'fsyms={coins}&'
        f'api_key={api}')
    url=multi+params
    r=requests.get(url).json()
    df=pd.DataFrame(r)
    c2=pd.json_normalize(df['RAW'])
    c2.head()
    CoinTable=c2
    col=CoinTable.columns
    for c in col:
        c2=c[4:]
        CoinTable=CoinTable.rename(columns={c:c2})
    
    return (CoinTable)

GetCoinList(coins)
    
CoinTable.head()  

engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                .format(host=hostname, db=dbname, user=uname, pw=pwd))

CoinTable.to_sql('CoinTable', engine, if_exists='replace', index=False)



coins='BTC,BNB,ETH,ADA,XRP,DOT,LTC,BUSD,BCH,HT,EOS,LINK,TRX,DASH,DOGE,OKB,XLM,RVN,ETC,UNI,NEO,USDT,ZEC,BSV,REN,SXP,ATOM,BTT,SUSHI,CAKE,AAVE,XEM,MIOTA,IOST,XTZ,USDC,XMR,ONT,VET,FIL,MATIC,FTT,OMG,ICX,ALGO,XVS,YFI,QTUM,BAKE,CRV,SOL,AVAX,BNT,CRO,ZIL,GLM,KSM,LUNA,EGLD,SRM,SNX,THETA,JST,BAND,CHZ,TMTG,TFUEL,ZRX,CVC,WAVES,GRT,MANA,BAT,HBAR,FTM,GT,ELF,COMP,WBTC,CELO,NPXS,DGB,YFII,PAY,ALPHA,FRONT,DVP,RUNE,OXT,RSR,KNC,LSK,MX,NANO,REP,COTI,ENJ,LOOM,LRC,KAVA'
extraParams='dogedash'



coins2=['BTC','BNB','ETC','ETH','DOGE','LTC']
def GetCoin(name):
    global coins2, coinmap
    coinmap={}
    hist='https://min-api.cryptocompare.com/data/v2/histoday?'
    api='XXXYOUR_API_KEY_HEREXXX'
    for coin in coins2:
        params=(
            'extraParams=DogeDash&'
            f'fsym={coin}&'
            'tsym=USD&'
            f'api_key={api}&'
            'limit=30')
        r2url=hist+params
        r=requests.get(r2url).json()
        r=pd.json_normalize(r)
        coindf=pd.DataFrame(r['Data.Data'])
        coindf=coindf.explode('Data.Data').reset_index(drop=True)
        coindf=pd.json_normalize(coindf['Data.Data'])
        coindf['symbol']=coin
        coinmap[coin]=coindf

    return (coinmap)
    
GetCoin(coins2)       

coinmap.keys()

CoinHistory=pd.concat(coinmap)

CoinHistory['time'] = CoinHistory['time'].apply(lambda x:datetime.fromtimestamp(x))
CoinHistory['time'] = [datetime.strftime(item, "%Y-%m-%dT%H:%M:%SZ") for item in CoinHistory['time']]
CoinHistory['time'] = CoinHistory['time'].astype("string")

CoinHIstory=CoinHistory.drop(['conversionType','conversionSymbol'],axis=1,inplace=True)

CoinHistory.to_sql('CoinHistory', engine, if_exists='replace', index=False)

CoinHistory['symbol']=CoinHistory['symbol'].astype("string")

pbrest = 'https://api.powerbi.com/beta/XXXXXXYOUR_POWERBI_SPECIAL_STREAMING_PATHXXX'
body = bytes(CoinHistory.to_json(orient='records'), encoding='utf-8')
req = requests.post(pbrest, body)
print(req)
CoinTable=CoinTable.rename(columns={'FROMSYMBOL':'SYMBOL'}).astype("string")
CoinTable.head()

CoinTable.dtypes

CoinTable['CHANGEPCT24HOUR']=CoinTable['CHANGEPCT24HOUR'].astype(float)
CoinTable['PRICE']=CoinTable['PRICE'].astype(float)
CoinTable['CHANGE24HOUR']=CoinTable['CHANGE24HOUR'].astype(float)
CoinTable['LOW24HOUR']=CoinTable['LOW24HOUR'].astype(float)
CoinTable['OPEN24HOUR']=CoinTable['OPEN24HOUR'].astype(float)
CoinTable['HIGH24HOUR']=CoinTable['HIGH24HOUR'].astype(float)
CoinTable['LOWDAY']=CoinTable['LOWDAY'].astype(float)
CoinTable['HIGHDAY']=CoinTable['HIGHDAY'].astype(float)
CoinTable['OPENDAY']=CoinTable['OPENDAY'].astype(float)
CoinTable['SYMBOL']=CoinTable['SYMBOL'].astype('string')


CoinTable.head()
CoinTable.dtypes

CoinTable=CoinTable[['SYMBOL','PRICE','OPENDAY','HIGHDAY','LOWDAY','OPEN24HOUR','HIGH24HOUR','LOW24HOUR','CHANGEPCT24HOUR','LASTUPDATE']]

print(CoinTable.dtypes)

pbrest2='https://api.powerbi.com/beta/XXXYOUR_POWERBI_SPECIAL_STREAMING_PATHXXX'
body = bytes(CoinTable.to_json(orient='records'), encoding='utf-8')
req = requests.post(pbrest2, body)








import pandas as pd
import os # para interagir com windows (comandos tipo ls)
import glob 
from utils_log import log_decorator

# uma funcao de extract que le e consolida os json
@log_decorator
def extrair_dados(pasta: str) -> pd.DataFrame:
    arquivos_json = glob.glob(os.path.join(pasta, '*.json')) #glob.glob para listar tudo dentro da pasta
    df_list = [pd.read_json(arquivo) for arquivo in arquivos_json] #loop para ler todos arquivos json e transformar em dataframe
    df_total = pd.concat(df_list, ignore_index=True) #concatena os dataframes do df_list e ignora o index 
    # na aws sempre vai concatenar pq separa por dia.   
    return df_total

# uma funcao que transforma
# adicionando coluna.
@log_decorator
def calcular_kpi_de_total_de_vendas(df: pd.DataFrame) -> pd.DataFrame:
    df["Total"] = df["Quantidade"] * df["Venda"]
    print(df)
    return df

# uma funcao que da load em csv ou parquet
@log_decorator
def carregar_dados(df: pd.DataFrame, format_saida: list): #procedure que só vai salvar, não tem retorno
    """
    parametro que vai ser ou "csv" ou "parquet" ou "os dois"
    """
    print(format_saida)
    for formato in format_saida:
        if formato == 'csv':    
            df.to_csv("dados.csv")
        if formato == 'parquet':    
            df.to_parquet("dados.parquet")
@log_decorator
def pipeline_calcular_kpi_de_vendas_consolidado(pasta_argumento: str, formato_de_saida: list): 
    data_frame = extrair_dados(pasta_argumento)
    data_frame_calculado = calcular_kpi_de_total_de_vendas(data_frame)
    carregar_dados(data_frame_calculado,formato_de_saida)

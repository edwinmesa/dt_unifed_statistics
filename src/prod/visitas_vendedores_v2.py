import pandas as pd
import math
import numpy as np



class StatisticsUnified:

    def __init__(self) -> None:
        pass

    def convert_and_extract(self, value):
        if pd.notna(value):
            value = value.replace(',', '.')[:8]
            return value
        else:
            return np.nan
    def extract_clients(self):        
        df_clientes = pd.read_parquet('/home/pydev/workflow/dt_unifed_statistics/data/clientes_b2b.parquet')
        # df_clientes = [(df_clientes['COD_CANAL']=="ON") & (df_clientes['COD_CANAL']=="OFF")]
        df_clientes = df_clientes[(df_clientes['COD_CANAL'] == 'OFF') | (df_clientes['COD_CANAL'] == 'ON')] 
        df_clientes = df_clientes[(df_clientes['IND_ESTADO_ACTIVO_CLIENTE'] == 1)] 
        df_clientes["LONGITUD"] = df_clientes['LONGITUD'].apply(self.convert_and_extract)
        df_clientes["LATITUD"]  = df_clientes['LATITUD'].apply(self.convert_and_extract)
        df_clientes['LONGITUD']   = pd.to_numeric(df_clientes['LONGITUD'], errors='coerce')
        df_clientes['LATITUD']    = pd.to_numeric(df_clientes['LATITUD'], errors='coerce')
        # print(df_clientes.tail(3))
        # df_clientes.info()
    
    def extract_calendar(self):
        df_calendario = pd.read_parquet('/home/pydev/workflow/dt_unifed_statistics/data/calendario_visitas.parquet')
        df_calendario = df_calendario.iloc[:, [0, 1, 3, 5, 7]]
        # Usar el método .melt() para "desapilar" las columnas de códigos
        df_calendario = pd.melt(df_calendario, id_vars=["FECHA", "COD_DIA VISITA"], value_vars=["COD SEMANAL",	"COD QUINCENAL","COD MENSUAL"], var_name="Tipo_Codigo", value_name="Codigo_Frecuencia")
        # Cambiar el nombre de la columna utilizando el método .rename()
        df_calendario.rename(columns={'COD_DIA VISITA': 'COD_DIA_VISITA','Codigo_Frecuencia':'COD_FRECUENCIA_VISITA', 'FECHA': 'FECHAS_VISITA', 'Tipo_Codigo':'TIPO_CODIGO'}, inplace=True)
        df_calendario['FECHAS_VISITA'] = pd.to_datetime(df_calendario['FECHAS_VISITA'], format= "%d/%m/%Y").astype(str)
        # df_calendario["type_2"] = "calendario_visitas"
        df_calendario = df_calendario.iloc[:, [0, 1, 3]]
        df_calendario.head(3)
        # df_calendario.info()


st = StatisticsUnified()
st.extract_clients()
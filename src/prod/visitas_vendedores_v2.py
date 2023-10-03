import pandas as pd
import math
import numpy as np
import unicodedata
import re


class StatisticsUnified:

    def __init__(self) -> None:
        self.df_clientes   = pd.read_parquet('/home/pydev/workflow/dt_unifed_statistics/data/clientes_b2b.parquet')
        self.df_calendario = pd.read_parquet('/home/pydev/workflow/dt_unifed_statistics/data/calendario_visitas.parquet')
        self.df_visitas    = pd.read_parquet('/home/pydev/workflow/dt_unifed_statistics/data/visitas_vendedores.parquet')

    def convert_and_extract(self, value):
        if pd.notna(value):
            value = value.replace(',', '.')[:8]
            return value
        else:
            return np.nan
        
    def mark_based_on_inicio_visita(self, row):
        return 0 if pd.isnull(row['Inicio_Visita']) else 1    
        
    def harversine_distance(self, lon1, lat1, lon2, lat2):
        # Radio de la Tierra en kilómetros
        R = 6371.0
        # Convierte las coordenadas de grados a radianes
        lat1 = math.radians(lat1)
        lon1 = math.radians(lon1)
        lat2 = math.radians(lat2)
        lon2 = math.radians(lon2)
        # Diferencias de latitud y longitud
        dlat = lat2 - lat1
        dlon = lon2 - lon1
        #Se usa la ecuación "a = [sin ² (dlat / 2) + cos (lat1)] x cos (lat2) x sin ² (dlong / 2)"
        a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
        #Distancia en metros
        distance = R * c *1000
        return distance
    
    # Define a function to remove diacritical marks and special characters
    def remove_diacritical_marks(self, text):
        if text is None:
            return None
        # Normalize the text to decompose diacritical marks
        normalized_text = unicodedata.normalize('NFKD', text)
        cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', normalized_text)
        return cleaned_text
        # Use unicodedata to remove diacritical marks from characters
        # text_without_marks = ''.join([c for c in unicodedata.normalize('NFD', text) if not unicodedata.combining(c)])
        # Remove commas from the resulting text
        # return text_without_marks.replace(',', '')
    
    def extract_clients(self):        
        self.df_clientes             = self.df_clientes[(self.df_clientes['COD_CANAL'] == 'OFF') | (self.df_clientes['COD_CANAL'] == 'ON')] 
        self.df_clientes             = self.df_clientes[(self.df_clientes['IND_ESTADO_ACTIVO_CLIENTE'] == 1)] 
        self.df_clientes["LONGITUD"] = self.df_clientes['LONGITUD'].apply(self.convert_and_extract)
        self.df_clientes["LATITUD"]  = self.df_clientes['LATITUD'].apply(self.convert_and_extract)
        self.df_clientes['LONGITUD'] = pd.to_numeric(self.df_clientes['LONGITUD'], errors='coerce')
        self.df_clientes['LATITUD']  = pd.to_numeric(self.df_clientes['LATITUD'], errors='coerce')
        return self.df_clientes
            
    def extract_calendar(self):
        self.df_calendario = self.df_calendario.iloc[:, [0, 1, 3, 5, 7]]
        # Usar el método .melt() para "desapilar" las columnas de códigos
        self.df_calendario = pd.melt(self.df_calendario, id_vars=["FECHA","COD_DIA VISITA"],value_vars=["COD SEMANAL","COD QUINCENAL","COD MENSUAL"], var_name="Tipo_Codigo", value_name="Codigo_Frecuencia")
        # Cambiar el nombre de la columna utilizando el método .rename()
        self.df_calendario.rename(columns={'COD_DIA VISITA': 'COD_DIA_VISITA','Codigo_Frecuencia':'COD_FRECUENCIA_VISITA', 'FECHA': 'FECHAS_VISITA', 'Tipo_Codigo':'TIPO_CODIGO'}, inplace=True)
        self.df_calendario['FECHAS_VISITA'] = pd.to_datetime(self.df_calendario['FECHAS_VISITA'], format= "%d/%m/%Y").astype(str)
        self.df_calendario = self.df_calendario.iloc[:, [0, 1, 3]]
        # print(self.df_calendario.head(3))
        return self.df_calendario
    
    def extract_visitas(self):
        self.df_visitas['FECHAS_VISITA']     = pd.to_datetime(self.df_visitas['Inicio_Visita'])
        self.df_visitas["Logintud"]          = self.df_visitas['Logintud'].apply(self.convert_and_extract)
        self.df_visitas["Latitud"]           = self.df_visitas['Latitud'].apply(self.convert_and_extract)
        self.df_visitas['AÑO']               = self.df_visitas['FECHAS_VISITA'].dt.year.astype(str)
        self.df_visitas                      = self.df_visitas[self.df_visitas["AÑO"]=='2023']
        self.df_visitas['FECHAS_VISITA']     = self.df_visitas['FECHAS_VISITA'].dt.date.astype(str)
        self.df_visitas['FECHAS_VISITA_V']   = self.df_visitas['FECHAS_VISITA']
        self.df_visitas                      = self.df_visitas.iloc[:, [5, 0, 1, 2, 3, 10, 12]]
        return self.df_visitas
    
    def transform_1(self):
        # Merge the customers with calendar()
        self.base_clientes_rutero               = pd.merge(self.df_clientes, self.df_calendario, how='outer')
        self.base_clientes_cruze                = self.df_clientes.copy()
        self.merge_cli_cruze                    = pd.merge(self.df_visitas, self.base_clientes_cruze, how="left")
        # Convert the datetime columns to datetime objects
        self.merge_cli_cruze['StartDatetime']   = pd.to_datetime(self.merge_cli_cruze['Inicio_Visita'], format='%m/%d/%Y %H:%M')
        self.merge_cli_cruze['EndDatetime']     = pd.to_datetime(self.merge_cli_cruze['Finalizacion_Visita'], format='%m/%d/%Y %H:%M')
        # df_result['Inicio_Visita_F'] = df_result['Inicio_Visita'].dt.date
        # Calculate the time difference
        self.merge_cli_cruze['TimeDifference']  = self.merge_cli_cruze['EndDatetime'] - self.merge_cli_cruze['StartDatetime']
        # Extract hours and minutes and format as a single column
        self.merge_cli_cruze['TimeDifferenceD'] = self.merge_cli_cruze['TimeDifference'].apply(lambda x: f"{x.seconds//3600}h:{x.seconds%3600//60} min")
        self.merge_cli_cruze['Time']            = self.merge_cli_cruze['TimeDifference'].apply(lambda x: f"{x.seconds//3600}{x.seconds%3600//60}")
        self.merge_cli_cruze.drop(['StartDatetime','EndDatetime'],axis=1, inplace=True)
        self.merge_cli_cruze['TimeDifference']  = ['' if x == 'nanh:nan min' else x for x in self.merge_cli_cruze['TimeDifference']]
        self.merge_cli_cruze['Time']            = pd.to_numeric(self.merge_cli_cruze['Time'], errors='coerce')
        self.merge_cli_cruze                    = self.merge_cli_cruze.sort_values(by=['ID_TERCERO','FECHAS_VISITA'], ascending=[True, False])
        self.merge_cli_cruze['Logintud']        = pd.to_numeric(self.merge_cli_cruze['Logintud'], errors='coerce')
        self.merge_cli_cruze['Latitud']         = pd.to_numeric(self.merge_cli_cruze['Latitud'], errors='coerce')

        #Arreglo para almacenar la distancia entre los puntos
        DistanciasHaversine =[]
        # Recorre las filas del DataFrame
        for indice, fila in self.merge_cli_cruze.iterrows():
            lon1 = fila['Logintud']
            lat1 = fila['Latitud']
            lon2 = fila['LONGITUD']
            lat2 = fila['LATITUD']
            #print(fila)
            if(lon1 != 0 and lon2 != 0 and lat1 != 0 and lat2 != 0):
                total_H = self.harversine_distance(lon1, lat1, lon2, lat2)
                DistanciasHaversine.append(total_H)
            else:
                DistanciasHaversine.append(0)

        self.merge_cli_cruze["Distancia_Haversine"] = DistanciasHaversine

        self.merge_cli_cruze['Rango_Visita'] = ['Dentro del Rango' if score <= 200 else ('Fuera del Rango' if score >= 200 else '') for score in self.merge_cli_cruze['Distancia_Haversine']]

        # print(self.merge_cli_cruze.head(5))
        return self.merge_cli_cruze

    def transform_2(self):
        self.result                                 = pd.merge(self.base_clientes_rutero, self.merge_cli_cruze, how="outer")
        self.result_example                         = self.result.sort_values(by=['ID_TERCERO', 'FECHAS_VISITA'], ascending=[True, True])
        self.result_example['Flag_Time']            = self.result_example.groupby(['ID_TERCERO', 'FECHAS_VISITA'])['Time'].rank(method='dense', ascending=False) # Ok
        self.result_example                         = self.result_example.sort_values(by=['ID_TERCERO', 'FECHAS_VISITA','Distancia_Haversine', 'Flag_Time'], ascending=[True, True, True, True])
        self.result_example['Flag_Distance']        = self.result_example.groupby(['ID_TERCERO', 'FECHAS_VISITA'])['Distancia_Haversine'].rank(method='min', ascending=False) # Ok
        self.result_example['Flag_DAN']             = self.result_example.apply(self.mark_based_on_inicio_visita, axis=1)

        self.result_example['Flag_Visita_Rutero']   = (self.result_example['FECHAS_VISITA'] == self.result_example['FECHAS_VISITA_V']).astype(int)

        self.result_example                         = self.result_example[self.result_example['ROWID_TERCERO'].notnull()]

        self.result_example['RAZON_SOCIAL']         = self.result_example['RAZON_SOCIAL'].apply(self.remove_diacritical_marks)
        self.result_example['DES_SUCURSAL_CLIENTE'] = self.result_example['DES_SUCURSAL_CLIENTE'].apply(self.remove_diacritical_marks)
        self.result_example['DES_DIA_VISITA']       = self.result_example['DES_DIA_VISITA'].apply(self.remove_diacritical_marks)
        self.result_example['DES_VENDEDOR']         = self.result_example['DES_VENDEDOR'].apply(self.remove_diacritical_marks)

        # print(self.result_example.head(5))
        return self.result_example
   
    def save_partition_file(self):
        # Calculate the number of rows in each part
        num_rows = len(self.result_example)
        num_parts = 3
        rows_per_part = num_rows // num_parts
        remainder = num_rows % num_parts  # Handles the remainder of rows
        # Initialize a list to store the parts
        parts = []
        # Split the DataFrame into 5 parts
        start_idx = 0
        for i in range(num_parts):
            if i < remainder:
                end_idx = start_idx + rows_per_part + 1
            else:
                end_idx = start_idx + rows_per_part
            
            part_df = self.result_example.iloc[start_idx:end_idx]
            parts.append(part_df)
            start_idx = end_idx
        # Save each part as a separate CSV file

        for i, part_df in enumerate(parts):
            part_df.to_csv(f'/home/pydev/workflow/dt_unifed_statistics/resultUnifiedStatistics/result_example_client_{i+1}.csv', mode='w', sep=';', encoding='latin1', errors='ignore', index=False)

st = StatisticsUnified()
st.extract_clients()
st.extract_calendar()
st.extract_visitas()
st.transform_1()
st.transform_2()
st.save_partition_file()
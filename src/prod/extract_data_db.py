# import library
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from pathlib import Path
import os, shutil, zipfile
from shutil import rmtree
import datetime as dt


# Route for save files generated
extract = f"/home/pydev/workflow/dt_unifed_statistics/data/"


sql_MEDPAG = """
SELECT F025_ID_CIA        AS CIA, 
       F025_ID            AS COD_MED_PAGO, 
       F025_DESCRIPCION   AS DES_MED_PAGO
  FROM T025_MM_MEDIOS_PAGO
"""

sqlClientes = """
	SELECT 
         T200.F200_ID_CIA               					          AS COD_CIA,
         T200.F200_ROWID                					          AS ROWID_TERCERO,
         T200.F200_NIT                  					          AS NIT_TERCERO,
         CONCAT(T200.F200_NIT,'-',T201.F201_ID_SUCURSAL)        AS ID_TERCERO,
         T200.F200_RAZON_SOCIAL         					          AS RAZON_SOCIAL,
         T201.F201_ID_SUCURSAL          					          AS COD_SUCURSAL_CLIENTE,
         T201.F201_DESCRIPCION_SUCURSAL 					          AS DES_SUCURSAL_CLIENTE,
         T201.F201_IND_ESTADO_ACTIVO    					          AS IND_ESTADO_ACTIVO_CLIENTE,
         CAST(TM_001.F206_ID AS CHAR)                 		    AS COD_DIA_VISITA,
         TM_001.F206_DESCRIPCION        					          AS DES_DIA_VISITA,
         T201.F201_ID_CO_FACTURA                                AS COD_CO,
         CONVERT(TM_002.F206_ID, CHAR)                 		    AS COD_FRECUENCIA_VISITA,
         TM_002.F206_DESCRIPCION        					          AS DES_FRECUENCIA_VISITA,
         TM_VEN.F206_ID        					                   AS COD_VENDEDOR,
         TM_VEN.F206_DESCRIPCION        					          AS DES_VENDEDOR,
         TM_CCD.F206_DESCRIPCION        					          AS COD_CANAL,
         T753_LG.F753_DATO_TEXTO			                         AS LONGITUD,
         T753_LT.F753_DATO_TEXTO			                         AS LATITUD
    FROM UNOEE.T200_MM_TERCEROS T200
   INNER JOIN UNOEE.T201_MM_CLIENTES T201
      ON T201.F201_ID_CIA = T200.F200_ID_CIA
     AND T201.F201_ROWID_TERCERO = T200.F200_ROWID
    INNER JOIN T750_MM_MOVTO_ENTIDAD T750
      ON T201.F201_ROWID_MOVTO_ENTIDAD = T750.F750_ROWID
   INNER JOIN T752_MM_MOVTO_ENTIDAD_FILA T752
      ON T750.F750_ROWID = T752.F752_ROWID_MOVTO_ENTIDAD
   INNER JOIN T742_MM_ENTIDAD T742
      ON T752.F752_ROWID_ENTIDAD = T742.F742_ROWID
   INNER JOIN T753_MM_MOVTO_ENTIDAD_COLUMNA T753_LG
      ON T753_LG.F753_ROWID_ENTIDAD = T742.F742_ROWID
     AND T753_LG.F753_ROWID_MOVTO_ENTIDAD_FILA = T752.F752_ROWID
     AND T753_LG.F753_ROWID_MOVTO_ENTIDAD = T750.F750_ROWID
   INNER JOIN T743_MM_ENTIDAD_ATRIBUTO T743_LG
      ON T743_LG.F743_ROWID_ENTIDAD = T742.F742_ROWID
     AND (CASE
           WHEN T743_LG.F743_ROWID IN ('881', '901') THEN
            T743_LG.F743_ROWID
           ELSE
            0
         END) = T753_LG.F753_ROWID_ENTIDAD_ATRIBUTO
   INNER JOIN T753_MM_MOVTO_ENTIDAD_COLUMNA T753_LT
      ON T753_LT.F753_ROWID_ENTIDAD = T742.F742_ROWID
     AND T753_LT.F753_ROWID_MOVTO_ENTIDAD_FILA = T752.F752_ROWID
     AND T753_LT.F753_ROWID_MOVTO_ENTIDAD = T750.F750_ROWID
   INNER JOIN T743_MM_ENTIDAD_ATRIBUTO T743_LT
      ON T743_LT.F743_ROWID_ENTIDAD = T742.F742_ROWID
     AND (CASE
           WHEN T743_LT.F743_ROWID IN ('882', '902') THEN
            T743_LT.F743_ROWID
           ELSE
            0
         END) = T753_LT.F753_ROWID_ENTIDAD_ATRIBUTO  
    LEFT OUTER JOIN UNOEE.T207_MM_CRITERIOS_CLIENTES TCC_001
      ON TCC_001.F207_ROWID_TERCERO = T201.F201_ROWID_TERCERO
     AND TCC_001.F207_ID_SUCURSAL = T201.F201_ID_SUCURSAL
     AND TCC_001.F207_ID_PLAN_CRITERIOS = '001'
    LEFT OUTER JOIN UNOEE.T206_MM_CRITERIOS_MAYORES TM_001
      ON TM_001.F206_ID_CIA = TCC_001.F207_ID_CIA
     AND TM_001.F206_ID_PLAN = TCC_001.F207_ID_PLAN_CRITERIOS
     AND TM_001.F206_ID = TCC_001.F207_ID_CRITERIO_MAYOR
    LEFT OUTER JOIN UNOEE.T204_MM_PLANES_CRITERIOS TPC_001
      ON TPC_001.F204_ID_CIA = TM_001.F206_ID_CIA
     AND TPC_001.F204_ID = TM_001.F206_ID_PLAN
    LEFT OUTER JOIN UNOEE.T207_MM_CRITERIOS_CLIENTES TCC_002
      ON TCC_002.F207_ROWID_TERCERO = T201.F201_ROWID_TERCERO
     AND TCC_002.F207_ID_SUCURSAL = T201.F201_ID_SUCURSAL
     AND TCC_002.F207_ID_PLAN_CRITERIOS = '002'
    LEFT OUTER JOIN UNOEE.T206_MM_CRITERIOS_MAYORES TM_002
      ON TM_002.F206_ID_CIA = TCC_002.F207_ID_CIA
     AND TM_002.F206_ID_PLAN = TCC_002.F207_ID_PLAN_CRITERIOS
     AND TM_002.F206_ID = TCC_002.F207_ID_CRITERIO_MAYOR
    LEFT OUTER JOIN UNOEE.T204_MM_PLANES_CRITERIOS TPC_002
      ON TPC_002.F204_ID_CIA = TM_002.F206_ID_CIA
     AND TPC_002.F204_ID = TM_002.F206_ID_PLAN
     
	LEFT OUTER JOIN UNOEE.T207_MM_CRITERIOS_CLIENTES TCC_CCD
      ON TCC_CCD.F207_ROWID_TERCERO = T201.F201_ROWID_TERCERO
     AND TCC_CCD.F207_ID_SUCURSAL = T201.F201_ID_SUCURSAL
     AND TCC_CCD.F207_ID_PLAN_CRITERIOS = 'CCD'
    LEFT OUTER JOIN UNOEE.T206_MM_CRITERIOS_MAYORES TM_CCD
      ON TM_CCD.F206_ID_CIA = TCC_CCD.F207_ID_CIA
     AND TM_CCD.F206_ID_PLAN = TCC_CCD.F207_ID_PLAN_CRITERIOS
     AND TM_CCD.F206_ID = TCC_CCD.F207_ID_CRITERIO_MAYOR
    LEFT OUTER JOIN UNOEE.T204_MM_PLANES_CRITERIOS TPC_CCD
      ON TPC_CCD.F204_ID_CIA = TM_CCD.F206_ID_CIA
     AND TPC_CCD.F204_ID = TM_CCD.F206_ID_PLAN

   LEFT OUTER JOIN UNOEE.T207_MM_CRITERIOS_CLIENTES TCC_VEN
      ON TCC_VEN.F207_ROWID_TERCERO = T201.F201_ROWID_TERCERO
     AND TCC_VEN.F207_ID_SUCURSAL = T201.F201_ID_SUCURSAL
     AND TCC_VEN.F207_ID_PLAN_CRITERIOS = 'VEN'
    LEFT OUTER JOIN UNOEE.T206_MM_CRITERIOS_MAYORES TM_VEN
      ON TM_VEN.F206_ID_CIA = TCC_VEN.F207_ID_CIA
     AND TM_VEN.F206_ID_PLAN = TCC_VEN.F207_ID_PLAN_CRITERIOS
     AND TM_VEN.F206_ID = TCC_VEN.F207_ID_CRITERIO_MAYOR
    LEFT OUTER JOIN UNOEE.T204_MM_PLANES_CRITERIOS TPC_VEN
      ON TPC_VEN.F204_ID_CIA = TM_VEN.F206_ID_CIA
     AND TPC_VEN.F204_ID = TM_VEN.F206_ID_PLAN   
    WHERE T200.F200_ID_CIA = 2 AND T742.F742_ROWID IN ('181', '201')

"""

sqlVisitas  = """
SELECT 
    visita.longitud                                                                                 AS Logintud,
    visita.latitud                                                                                  AS Latitud,
    visita.fecha_inicio                                                                             AS Inicio_Visita,
    visita.fecha_final                                                                              AS Finalizacion_Visita,
    CONCAT(tercero.nombre_completo,' ',tercero.primer_apellido,' ',tercero.segundo_apellido)        AS Razon_Social_Cliente,
    CONCAT(tercero.numero_identificacion ,'-',sucursal.numero_sucursal)							    AS ID_TERCERO,
    tercero.numero_identificacion                                                                   AS Nit,
    sucursal.numero_sucursal                                                                        AS Codigo_Sucursal,
	vendedor.numero_identificacion                                                                   AS Codigo_Vendedor,
    CONCAT(vendedor.primer_apellido,' ',vendedor.nombre_completo,' ',vendedor.segundo_apellido)     AS Des_vendedor
FROM
    ORIONB2B.VISITA visita
        INNER JOIN
    ORIONB2B.USUARIO tercero ON visita.tercero_id = tercero.usuario_id
        INNER JOIN
    ORIONB2B.USUARIO vendedor ON visita.vendedor_id = vendedor.usuario_id
        INNER JOIN
    ORIONB2B.SUCURSAL sucursal ON visita.sucursal_id = sucursal.sucursal_id
WHERE
    visita.longitud != 0 AND visita.latitud != 0
"""


db_username    = "edwinmesa"
db_password    = "Edwsar2023."
db_host        = "dis-rds-mysql-cluster-pdn.cluster-c0f2phco5xvu.us-east-1.rds.amazonaws.com"
db_port        = "3407"  # Replace with your Aurora's port (e.g., 3306 for MySQL)
db_name_unoee  = "UNOEE"
db_name_orion  = "ORIONB2B"

 # Conect database an saved files
try:
   connection_url = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name_unoee}"
   engine         = create_engine(connection_url)
   df             = pd.read_sql_query(con=engine.connect(), sql=text(sqlClientes))
   df.to_parquet( extract + 'clientes_b2b.parquet', index=None)

except SQLAlchemyError as e:
   print(e) 


try:
   connection_url = f"mysql+mysqlconnector://{db_username}:{db_password}@{db_host}:{db_port}/{db_name_orion}"
   engine         = create_engine(connection_url)
   df             = pd.read_sql_query(con=engine.connect(), sql=text(sqlVisitas))
   df.to_parquet( extract + 'visitas_vendedores.parquet', index=None)
  
except SQLAlchemyError as e:
   print(e)    



df_calendario_visitas = pd.read_csv('/home/pydev/workflow/dt_unifed_statistics/data/Calendario_Pimovil.csv', dtype=str) 
df_calendario_visitas.to_parquet('/home/pydev/workflow/dt_unifed_statistics/data/calendario_visitas.parquet')
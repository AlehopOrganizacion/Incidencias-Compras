import streamlit as st
import pandas as pd
import os
import re
from datetime import datetime
from collections import Counter
import io
import numpy as np
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError

# --- LIBRERÍAS AÑADIDAS PARA EL PDF ---
import matplotlib.pyplot as plt
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.platypus import Image as ReportLabImage
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch


# --- Configuración de la página ---
st.set_page_config(
    page_title="Gestión de incidencias",
    page_icon="⚠️",
    layout="wide"
)

# --- Listas de opciones para desplegables ---
ESTADOS = ["PDTE. ENVIO INCIDENCIA", "EN REVISIÓN", "ESPERANDO MATERIAL", "TRABAJOS EN ALMACÉN", "CONTEO COSTES", "COMPLETADO", "REVISIÓN AQL", "🟢 AQL POSITIVO", "🔴 AQL NEGATIVO"]
PROCEDENCIAS = ["OLIVA", "LLEGADAS", "PRESCRIPCIÓN", "TIENDAS", "ORIGEN", "COMPRAS", "INSPECCIÓN"]
CATEGORIAS = ["DIVERTIDO", "INFANTIL", "BIENESTAR Y DEPORTE", "ELECTRONICA", "HOGAR", "PAPELERIA", "MODA", "INTENDENCIA"]
ORIGENES_INCIDENCIA = ["PROVEEDOR", "ALE HOP", "NO APLICA"]
TIPOS_INCIDENCIA = ["PRODUCTO", "PACKAGING", "CANTIDAD", "SUGERENCIA", "RETRASO"]
ESTADOS_BLOQUEO = ["BLOQUEADO", "APTO", "RETIRADO"]
ESTADOS_INSPECCION = ["Pendiente", "Finalizada"]

# --- CONFIGURACIÓN DE BASE DE DATOS POSTGRESQL ---
# Obtener URL de la base de datos desde variable de entorno
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://usuario:password@localhost:5432/incidencias_db')

# Si DATABASE_URL viene en formato de Heroku/Coolify, ajustar para SQLAlchemy
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

# --- CONFIGURACIÓN PARA MODO DE DATOS ---
# Si USE_DB_FOR_EXCEL_DATA es True, los datos se leen de la BD en lugar de archivos Excel
USE_DB_FOR_EXCEL_DATA = os.getenv('USE_DB_FOR_EXCEL_DATA', 'True').lower() == 'true'

# --- RUTAS A EXCELS (solo si no se usa BD) ---
EXCEL_DIRECTORY = os.getenv('EXCEL_DIRECTORY', r"V:\AREA CALIDAD Y TEST REPORT\10-AREA CALIDAD\0. DESARROLLO APPS\EXCELS")

def get_db_engine():
    """Crea y devuelve un engine de SQLAlchemy para PostgreSQL."""
    try:
        engine = create_engine(DATABASE_URL, pool_pre_ping=True)
        return engine
    except Exception as e:
        st.error(f"Error al conectar con la base de datos: {e}")
        return None

def init_all_dbs():
    """Inicializa y actualiza TODAS las tablas si es necesario."""
    engine = get_db_engine()
    if not engine:
        return
    
    try:
        with engine.connect() as conn:
            # Crear tablas de llegadas
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS llegadas (
                    id SERIAL PRIMARY KEY,
                    PEDIDO TEXT NOT NULL,
                    DESCRIPCION TEXT,
                    REFERENCIA TEXT NOT NULL,
                    CANTIDAD INTEGER,
                    MIDDLE INTEGER,
                    FECHA_LLEGADA TEXT,
                    LOTE TEXT,
                    ESTADO_INSPECCION TEXT DEFAULT 'Pendiente'
                )
            '''))
            
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS llegadas_ignoradas (
                    id SERIAL PRIMARY KEY,
                    PEDIDO TEXT NOT NULL,
                    DESCRIPCION TEXT,
                    REFERENCIA TEXT NOT NULL,
                    CANTIDAD INTEGER,
                    MIDDLE INTEGER,
                    FECHA_LLEGADA TEXT,
                    LOTE TEXT
                )
            '''))
            
            # Crear tabla de incidencias pendientes
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS listado_incidencias (
                    id SERIAL PRIMARY KEY,
                    PEDIDO TEXT,
                    LOTE TEXT,
                    REFERENCIA TEXT,
                    DESCRIPCION TEXT,
                    CANTIDAD_LLEGADA INTEGER,
                    FECHA_LLEGADA TEXT,
                    COMENTARIOS_CALIDAD TEXT,
                    TOTAL_NC INTEGER DEFAULT 0,
                    MENORES INTEGER DEFAULT 0,
                    MAYORES INTEGER DEFAULT 0,
                    CRITICOS INTEGER DEFAULT 0
                )
            '''))
            
            # Crear tabla de incidencias activas con todas las columnas
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS incidencias_activas (
                    id SERIAL PRIMARY KEY,
                    RESPONSABLE_CALIDAD TEXT,
                    ESTADO TEXT,
                    PROCEDENCIA TEXT,
                    CATEGORIA TEXT,
                    ORIGEN_INCIDENCIA TEXT,
                    TIPO_INCIDENCIA TEXT,
                    PRIORIDAD TEXT,
                    FECHA_APERTURA TEXT,
                    FECHA_FIN TEXT,
                    PEDIDO TEXT,
                    LOTE TEXT,
                    MARCA TEXT,
                    REFERENCIA TEXT,
                    DESCRIPCION TEXT,
                    PICKING TEXT,
                    CANTIDAD_LLEGADA INTEGER,
                    BULTOS INTEGER,
                    MASTER INTEGER,
                    MIDDLE INTEGER,
                    PIEZAS_A_REVISAR INTEGER,
                    MASTERS_DIFERENTES TEXT,
                    COMENTARIOS TEXT,
                    ACCIONES TEXT,
                    BLOQUEO_REFERENCIAS TEXT,
                    FECHA_INICIO_TAREA TEXT,
                    FECHA_PREVISION_FIN TEXT,
                    COSTE_MATERIAL_Y_PERSONAL DECIMAL(10,2),
                    RECUPERADO_PROVEEDOR DECIMAL(10,2)
                )
            '''))
            
            # Crear tabla de incidencias rechazadas
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS incidencias_rechazadas (
                    id SERIAL PRIMARY KEY,
                    PEDIDO TEXT,
                    LOTE TEXT,
                    REFERENCIA TEXT,
                    DESCRIPCION TEXT,
                    CANTIDAD_LLEGADA INTEGER,
                    FECHA_LLEGADA TEXT,
                    COMENTARIOS_CALIDAD TEXT,
                    FECHA_RECHAZO TEXT
                )
            '''))
            
            # Crear tabla para datos de Ale-hop (ventas y mermas)
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS alehop_data (
                    id SERIAL PRIMARY KEY,
                    product_id TEXT NOT NULL,
                    ano_mes_no DATE,
                    pcs_sold NUMERIC,
                    broken_pcs NUMERIC,
                    categoria TEXT,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(product_id, ano_mes_no)
                )
            '''))
            
            # Crear índice para mejorar rendimiento
            conn.execute(text('''
                CREATE INDEX IF NOT EXISTS idx_alehop_product_id ON alehop_data(product_id);
            '''))
            
            # Crear tabla para BBDD de artículos
            conn.execute(text('''
                CREATE TABLE IF NOT EXISTS bbdd_articulos (
                    id SERIAL PRIMARY KEY,
                    referencia TEXT UNIQUE NOT NULL,
                    fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''))
            
            conn.commit()
            
            # Verificar y añadir columnas faltantes
            inspector = inspect(engine)
            
            # Verificar columnas en llegadas
            llegadas_columns = [col['name'] for col in inspector.get_columns('llegadas')]
            if 'lote' not in llegadas_columns and 'LOTE' not in llegadas_columns:
                conn.execute(text("ALTER TABLE llegadas ADD COLUMN LOTE TEXT"))
            if 'estado_inspeccion' not in llegadas_columns and 'ESTADO_INSPECCION' not in llegadas_columns:
                conn.execute(text("ALTER TABLE llegadas ADD COLUMN ESTADO_INSPECCION TEXT DEFAULT 'Pendiente'"))
            
            # Verificar columnas en llegadas_ignoradas
            ignoradas_columns = [col['name'] for col in inspector.get_columns('llegadas_ignoradas')]
            if 'lote' not in ignoradas_columns and 'LOTE' not in ignoradas_columns:
                conn.execute(text("ALTER TABLE llegadas_ignoradas ADD COLUMN LOTE TEXT"))
            
            conn.commit()
            
    except Exception as e:
        st.error(f"Error al inicializar las tablas: {e}")

@st.cache_data
def obtener_estado_articulos():
    """
    Lee los ficheros de Ale-hop y BBDD para determinar si un artículo
    es Novedad o Recompra, según las nuevas reglas.
    """
    if USE_DB_FOR_EXCEL_DATA:
        # Leer desde la base de datos
        engine = get_db_engine()
        if not engine:
            return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])
        
        try:
            # Obtener referencias de alehop_data
            query_alehop = "SELECT DISTINCT product_id as referencia FROM alehop_data"
            df_alehop = pd.read_sql_query(query_alehop, engine)
            alehop_refs = set(df_alehop['referencia'].dropna().astype(str))
            
            # Obtener referencias de bbdd_articulos
            query_bbdd = "SELECT referencia FROM bbdd_articulos"
            df_bbdd = pd.read_sql_query(query_bbdd, engine)
            bbdd_refs = set(df_bbdd['referencia'].dropna().astype(str))
            
            all_refs = bbdd_refs.union(alehop_refs)
            resultados = []
            for ref in all_refs:
                if ref in alehop_refs:
                    tipo = "Recompra"
                elif ref in bbdd_refs:
                    tipo = "Novedad"
                else:
                    continue
                
                resultados.append({'REFERENCIA': ref, 'TIPO_PEDIDO': tipo})
            
            return pd.DataFrame(resultados)
            
        except Exception as e:
            st.error(f"Error al leer datos de artículos desde la BD: {e}")
            return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])
    
    else:
        # Código original para leer desde archivos Excel
        alehop_path = os.path.join(EXCEL_DIRECTORY, "Ale-hop Data fecha y cat.xlsx")
        bbdd_path = os.path.join(EXCEL_DIRECTORY, "BBDD ARTICULOS.xlsx")
        
        if not os.path.exists(alehop_path) or not os.path.exists(bbdd_path):
            st.warning("Faltan 'Ale-hop Data' o 'BBDD ARTICULOS'. No se pudo determinar el tipo de pedido.")
            return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])
        
        try:
            df_alehop = pd.read_excel(alehop_path, engine='openpyxl')
            df_alehop.columns = [str(c).upper().strip() for c in df_alehop.columns]
            if 'PRODUCT ID' in df_alehop.columns:
                alehop_refs = set(df_alehop['PRODUCT ID'].dropna().astype(str))
            else:
                st.error("No se encontró la columna 'PRODUCT ID' en 'Ale-hop Data'.")
                return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])

            df_bbdd = pd.read_excel(bbdd_path, engine='openpyxl', header=None)
            if df_bbdd.shape[1] > 1:
                bbdd_refs = set(df_bbdd.iloc[:, 1].dropna().astype(str))
            else:
                st.error("El fichero 'BBDD ARTICULOS.xlsx' no tiene el formato esperado (Columna B).")
                return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])
            
            all_refs = bbdd_refs.union(alehop_refs)
            resultados = []
            for ref in all_refs:
                if ref in alehop_refs:
                    tipo = "Recompra"
                elif ref in bbdd_refs:
                    tipo = "Novedad"
                else:
                    continue
                
                resultados.append({'REFERENCIA': ref, 'TIPO_PEDIDO': tipo})
                
            return pd.DataFrame(resultados)
            
        except Exception as e:
            st.error(f"Error al procesar los ficheros de artículos: {e}")
            return pd.DataFrame(columns=['REFERENCIA', 'TIPO_PEDIDO'])

@st.cache_data
def obtener_datos_merma():
    """
    Calcula el % de merma usando una Media Móvil Exponencial (EMA)
    para dar más peso a los datos más recientes.
    """
    if USE_DB_FOR_EXCEL_DATA:
        # Leer desde la base de datos
        engine = get_db_engine()
        if not engine:
            return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])
        
        try:
            query = """
                SELECT product_id as REFERENCIA, 
                       ano_mes_no as FECHA,
                       pcs_sold as "PCS SOLD",
                       broken_pcs as "BROKEN PCS"
                FROM alehop_data
                WHERE ano_mes_no IS NOT NULL
                ORDER BY product_id, ano_mes_no
            """
            df = pd.read_sql_query(query, engine)
            
            if df.empty:
                st.warning("No hay datos de merma en la base de datos.")
                return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])
            
            df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
            df.dropna(subset=['FECHA'], inplace=True)
            df.sort_values(by=['REFERENCIA', 'FECHA'], inplace=True)

            df['PCS SOLD'] = pd.to_numeric(df['PCS SOLD'], errors='coerce').fillna(0).abs()
            df['BROKEN PCS'] = pd.to_numeric(df['BROKEN PCS'], errors='coerce').fillna(0).abs()

            df['MERMA_PERIODO'] = 0.0
            df.loc[df['PCS SOLD'] > 0, 'MERMA_PERIODO'] = (df['BROKEN PCS'] / df['PCS SOLD']) * 100

            df['% MERMA_EMA'] = df.groupby('REFERENCIA')['MERMA_PERIODO'].transform(
                lambda x: x.ewm(alpha=0.3, adjust=False).mean()
            )

            info = df.loc[df.groupby('REFERENCIA')['FECHA'].idxmax()]
            info['% MERMA'] = info['% MERMA_EMA'].apply(lambda x: max(0, x - 5)).round(2)
            
            return info[['REFERENCIA', '% MERMA']]
            
        except Exception as e:
            st.error(f"Error al procesar datos de merma desde la BD: {e}")
            return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])
    
    else:
        # Código original para leer desde archivos Excel
        excel_path = os.path.join(EXCEL_DIRECTORY, "Ale-hop Data fecha y cat.xlsx")
        
        if not os.path.exists(excel_path):
            st.warning("No se encontró 'Ale-hop Data fecha y cat.xlsx'. No se podrá calcular el % de merma.")
            return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])

        try:
            df = pd.read_excel(excel_path, engine='openpyxl')
            df.columns = [str(c).upper().strip() for c in df.columns]

            required_cols = {'PRODUCT ID', 'AÑO MES NO', 'PCS SOLD', 'BROKEN PCS'}
            if not required_cols.issubset(df.columns):
                st.error(f"Faltan columnas en el Excel. Se necesitan: {required_cols}")
                return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])

            df.rename(columns={'PRODUCT ID': 'REFERENCIA', 'AÑO MES NO': 'FECHA'}, inplace=True)
            df['FECHA'] = pd.to_datetime(df['FECHA'], errors='coerce')
            df.dropna(subset=['FECHA'], inplace=True)
            df.sort_values(by=['REFERENCIA', 'FECHA'], inplace=True)

            df['PCS SOLD'] = pd.to_numeric(df['PCS SOLD'], errors='coerce').fillna(0).abs()
            df['BROKEN PCS'] = pd.to_numeric(df['BROKEN PCS'], errors='coerce').fillna(0).abs()

            df['MERMA_PERIODO'] = 0.0
            df.loc[df['PCS SOLD'] > 0, 'MERMA_PERIODO'] = (df['BROKEN PCS'] / df['PCS SOLD']) * 100

            df['% MERMA_EMA'] = df.groupby('REFERENCIA')['MERMA_PERIODO'].transform(
                lambda x: x.ewm(alpha=0.3, adjust=False).mean()
            )

            info = df.loc[df.groupby('REFERENCIA')['FECHA'].idxmax()]

            info['% MERMA'] = info['% MERMA_EMA'].apply(lambda x: max(0, x - 5)).round(2)
            
            return info[['REFERENCIA', '% MERMA']]

        except Exception as e:
            st.error(f"Error crítico al procesar el fichero de mermas: {e}")
            return pd.DataFrame(columns=['REFERENCIA', '% MERMA'])

def get_table_as_df(table_name, order_by="id DESC"):
    """Lee una tabla completa y devuelve un DataFrame."""
    engine = get_db_engine()
    if not engine:
        return pd.DataFrame()
    
    try:
        query = f"SELECT * FROM {table_name} ORDER BY {order_by}"
        return pd.read_sql_query(query, engine)
    except Exception as e:
        st.error(f"Error al leer la tabla {table_name}: {e}")
        return pd.DataFrame()

def save_df_to_table(df, table_name, check_duplicates=False, subset_cols=None):
    """Guarda un DataFrame en una tabla."""
    engine = get_db_engine()
    if not engine:
        return False
    
    try:
        if check_duplicates and subset_cols:
            existing_df = get_table_as_df(table_name)
            if not existing_df.empty:
                # Normalizar columnas para comparación
                for col in subset_cols:
                    if col in df.columns and col in existing_df.columns:
                        df[col] = df[col].astype(str)
                        existing_df[col] = existing_df[col].astype(str)
                
                # Filtrar duplicados
                df = df.merge(existing_df[subset_cols].drop_duplicates(), 
                            on=subset_cols, how='left', indicator=True)
                df = df[df['_merge'] == 'left_only'].drop(columns=['_merge'])
        
        if not df.empty:
            df.to_sql(table_name, engine, if_exists='append', index=False)
        return True
    except Exception as e:
        st.error(f"Error al guardar en la tabla {table_name}: {e}")
        return False

def update_record(table_name, record_id, updates):
    """Actualiza un registro específico en una tabla."""
    engine = get_db_engine()
    if not engine:
        return False
    
    try:
        # Convertir timestamps a strings
        for key, value in updates.items():
            if isinstance(value, pd.Timestamp):
                updates[key] = value.strftime('%Y-%m-%d')
        
        # Construir query de actualización
        set_clause = ", ".join([f'"{key}" = :{key}' for key in updates.keys()])
        query = f'UPDATE {table_name} SET {set_clause} WHERE id = :record_id'
        
        with engine.connect() as conn:
            updates['record_id'] = record_id
            conn.execute(text(query), updates)
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al actualizar el registro {record_id}: {e}")
        return False

def check_existing_llegadas(df_llegadas):
    """
    Comprueba las llegadas contra la BBDD de forma robusta,
    limpiando espacios y normalizando a mayúsculas para evitar falsos negativos.
    """
    engine = get_db_engine()
    if not engine:
        return pd.DataFrame(), 0
    
    try:
        # Normalizar datos de entrada
        df_llegadas['REFERENCIA_NORM'] = df_llegadas['REFERENCIA'].astype(str).str.strip().str.upper()
        df_llegadas['LOTE_NORM'] = df_llegadas['LOTE'].astype(str).str.strip()
        df_llegadas['unique_id'] = df_llegadas['REFERENCIA_NORM'] + '-' + df_llegadas['LOTE_NORM']
        
        # Leer las tablas existentes
        df_db_inspeccionar = pd.read_sql_query("SELECT REFERENCIA, LOTE FROM llegadas", engine)
        df_db_ignoradas = pd.read_sql_query("SELECT REFERENCIA, LOTE FROM llegadas_ignoradas", engine)
        
        existing_ids = set()
        
        # Normalizar datos de la BD
        if not df_db_inspeccionar.empty:
            ref_ins = df_db_inspeccionar['REFERENCIA'].astype(str).str.strip().str.upper()
            lote_ins = df_db_inspeccionar['LOTE'].astype(str).str.strip()
            existing_ids.update(set(ref_ins + '-' + lote_ins))
        
        if not df_db_ignoradas.empty:
            ref_ign = df_db_ignoradas['REFERENCIA'].astype(str).str.strip().str.upper()
            lote_ign = df_db_ignoradas['LOTE'].astype(str).str.strip()
            existing_ids.update(set(ref_ign + '-' + lote_ign))
        
        # Filtrar nuevas llegadas
        nuevas_llegadas_df = df_llegadas[~df_llegadas['unique_id'].isin(existing_ids)].copy()
        
        # Limpiar columnas temporales
        nuevas_llegadas_df.drop(columns=['REFERENCIA_NORM', 'LOTE_NORM', 'unique_id'], inplace=True, errors='ignore')
        df_llegadas.drop(columns=['REFERENCIA_NORM', 'LOTE_NORM', 'unique_id'], inplace=True, errors='ignore')
        
        return nuevas_llegadas_df, len(df_llegadas) - len(nuevas_llegadas_df)
        
    except Exception as e:
        st.error(f"Error al comprobar llegadas existentes: {e}")
        return pd.DataFrame(), 0

def delete_from_table_by_id(table_name, record_id):
    """Elimina un registro de una tabla por su ID."""
    engine = get_db_engine()
    if not engine:
        return False
    
    try:
        with engine.connect() as conn:
            query = text(f"DELETE FROM {table_name} WHERE id = :record_id")
            conn.execute(query, {"record_id": record_id})
            conn.commit()
        return True
    except Exception as e:
        st.error(f"Error al eliminar el registro {record_id} de la tabla {table_name}: {e}")
        return False

# --- Inicializar la BD ---
init_all_dbs()

# --- Gestión del estado de la sesión ---
if 'df_para_editar' not in st.session_state:
    st.session_state.df_para_editar = None
if 'file_uploader_key' not in st.session_state:
    st.session_state.file_uploader_key = 0

# --- UI de la Aplicación ---
st.sidebar.title("Menú de navegación")
app_mode = st.sidebar.radio(
    "Selecciona una sección:",
    ["Carga y revisión de llegadas", "Consultas de inspecciones y AQL", "Consultar llegadas ignoradas", 
     "Listado de incidencias", "Seguimiento de incidencias activas", "Consultar incidencias rechazadas",
     "Indicadores", "Gestión de datos maestros"] 
)

if app_mode == "Carga y revisión de llegadas":
    st.title("1️⃣ Carga y revisión de llegadas")
    if st.session_state.df_para_editar is None:
        with st.container(border=True):
            st.header("Cargar fichero de llegadas")
            uploaded_file = st.file_uploader("Sube aquí el fichero de llegadas.", type=['xlsx', 'csv'], key=f"uploader_{st.session_state.file_uploader_key}")
            if uploaded_file:
                try:
                    match = re.search(r'(\d{2}-\d{2}-\d{2,4})', uploaded_file.name)
                    fecha_llegada = datetime.strptime(match.group(1), '%d-%m-%y') if match else datetime.now()
                    df_original = pd.read_csv(uploaded_file) if uploaded_file.name.endswith('.csv') else pd.read_excel(uploaded_file)
                    
                    df_original.columns = df_original.columns.str.strip()
                    columnas_requeridas = ['PEDIDO', 'DESCRIPCIÓN', 'REFERENCIA', 'CANTIDAD', 'MIDDLE']
                    missing_cols = [col for col in columnas_requeridas if col not in df_original.columns]

                    if missing_cols:
                        st.error(f"El archivo subido no contiene las siguientes columnas necesarias: {', '.join(missing_cols)}")
                    else:
                        df_llegadas = df_original[columnas_requeridas].copy()
                        df_llegadas.rename(columns={'DESCRIPCIÓN': 'DESCRIPCION'}, inplace=True)
                        
                        df_llegadas['LOTE'] = df_llegadas['PEDIDO'].astype(str).str[-8:]

                        df_nuevas, num_duplicados = check_existing_llegadas(df_llegadas)
                        if num_duplicados > 0:
                            st.warning(f"Se encontraron y omitieron **{num_duplicados}** registros que ya existían.")
                        if df_nuevas.empty:
                            st.info("Todos los registros del archivo ya se encuentran en la base de datos.")
                        else:
                            st.success(f"Archivo procesado. Se han encontrado **{len(df_nuevas)}** registros nuevos para revisar.")
                            
                            df_nuevas['REFERENCIA'] = df_nuevas['REFERENCIA'].astype(str).str.strip()
                            
                            df_mermas = obtener_datos_merma()
                            df_tipos_pedido = obtener_estado_articulos()

                            if not df_mermas.empty:
                                df_mermas['REFERENCIA'] = df_mermas['REFERENCIA'].astype(str).str.strip()
                                df_nuevas = pd.merge(df_nuevas, df_mermas, on='REFERENCIA', how='left')
                            
                            if not df_tipos_pedido.empty:
                                df_tipos_pedido['REFERENCIA'] = df_tipos_pedido['REFERENCIA'].astype(str).str.strip()
                                df_nuevas = pd.merge(df_nuevas, df_tipos_pedido, on='REFERENCIA', how='left')
                            
                            if '% MERMA' not in df_nuevas.columns:
                                df_nuevas['% MERMA'] = 0
                            else:
                                df_nuevas['% MERMA'] = df_nuevas['% MERMA'].fillna(0)
                            
                            if 'TIPO_PEDIDO' not in df_nuevas.columns:
                                df_nuevas['TIPO_PEDIDO'] = 'Novedad'
                            else:
                                df_nuevas['TIPO_PEDIDO'] = df_nuevas['TIPO_PEDIDO'].fillna('Novedad')

                            df_incidencias = get_table_as_df('incidencias_activas')
                            if not df_incidencias.empty:
                                df_incidencias_abiertas = df_incidencias[df_incidencias['ESTADO'] != 'COMPLETADO'].copy()
                                if not df_incidencias_abiertas.empty:
                                    df_feedback = df_incidencias_abiertas[['REFERENCIA', 'COMENTARIOS']].copy()
                                    df_feedback.rename(columns={'COMENTARIOS': 'INCIDENCIA_ABIERTA'}, inplace=True)
                                    df_feedback.dropna(subset=['INCIDENCIA_ABIERTA'], inplace=True)
                                    df_feedback = df_feedback[df_feedback['INCIDENCIA_ABIERTA'] != '']
                                    df_feedback = df_feedback.groupby('REFERENCIA')['INCIDENCIA_ABIERTA'].apply(lambda x: ' | '.join(x)).reset_index()
                                    df_nuevas = pd.merge(df_nuevas, df_feedback, on='REFERENCIA', how='left')
                            
                            df_nuevas['INCIDENCIA_ABIERTA'] = df_nuevas.get('INCIDENCIA_ABIERTA', pd.Series(dtype='str')).fillna("No hay")
                            df_nuevas['ESTADO_REVISION'] = 'Producto a revisar'
                            df_nuevas['FECHA_LLEGADA'] = fecha_llegada.strftime('%Y-%m-%d')
                            
                            st.session_state.df_para_editar = df_nuevas
                            st.rerun()
                except Exception as e:
                    st.error(f"Ocurrió un error al procesar el archivo: {e}")
    else:
        with st.container(border=True):
            st.header("Revisar y guardar nuevas llegadas")
            st.info("Decide qué productos necesitan pasar por una inspección de calidad.")
            df_display = st.session_state.df_para_editar.copy()

            def format_tipo_pedido_visual(tipo):
                if tipo == 'Novedad':
                    return '🔵 Novedad'
                elif tipo == 'Recompra':
                    return '🟢 Recompra'
                return tipo

            if 'TIPO_PEDIDO' in df_display.columns:
                df_display.insert(0, 'Tipo', df_display['TIPO_PEDIDO'].apply(format_tipo_pedido_visual))

            edited_df = st.data_editor(
                df_display,
                column_config={
                    "TIPO_PEDIDO": None,
                    "Tipo": st.column_config.Column("Tipo de Pedido", disabled=True, help="**🔵 Novedad**: Primera vez que llega. **🟢 Recompra**: Ya tiene historial."),
                    "ESTADO_REVISION": st.column_config.SelectboxColumn("Acción", options=["Producto a revisar", "No es necesario revisar"], required=True),
                    "% MERMA": st.column_config.ProgressColumn(
                        "% Merma Corregida",
                        help="Porcentaje de merma histórico y dinámico (ajustado un -5%)",
                        format="%.2f%%",
                        min_value=0, max_value=100,
                    ),
                    "INCIDENCIA_ABIERTA": st.column_config.Column("Incidencia abierta", disabled=True)
                },
                use_container_width=True, num_rows="dynamic",
                column_order=["REFERENCIA", "DESCRIPCION", "CANTIDAD", "Tipo", "ESTADO_REVISION", "% MERMA", "INCIDENCIA_ABIERTA", "PEDIDO", "LOTE"]
            )
            st.divider()
            col1, col2 = st.columns(2)
            if col1.button("Guardar revisión", type="primary", use_container_width=True):
                if 'Tipo' in edited_df.columns:
                    df_a_guardar = edited_df.drop(columns=['Tipo'])
                else:
                    df_a_guardar = edited_df

                df_a_revisar = df_a_guardar[df_a_guardar['ESTADO_REVISION'] == 'Producto a revisar']
                df_no_necesario = df_a_guardar[df_a_guardar['ESTADO_REVISION'] == 'No es necesario revisar']
                
                cols_to_drop = ['ESTADO_REVISION', 'INCIDENCIA_ABIERTA', '% MERMA', 'TIPO_PEDIDO']
                
                if not df_a_revisar.empty:
                    if save_df_to_table(df_a_revisar.drop(columns=cols_to_drop, errors='ignore'), 'llegadas'):
                        st.success(f"Se han guardado **{len(df_a_revisar)}** productos para inspección.")
                if not df_no_necesario.empty:
                    if save_df_to_table(df_no_necesario.drop(columns=cols_to_drop, errors='ignore'), 'llegadas_ignoradas'):
                        st.info(f"Se han guardado **{len(df_no_necesario)}** productos como ignorados.")
                
                st.session_state.df_para_editar = None
                st.session_state.file_uploader_key += 1
                import time
                time.sleep(2)
                st.rerun()
            if col2.button("Cancelar", use_container_width=True):
                st.session_state.df_para_editar = None
                st.session_state.file_uploader_key += 1
                st.rerun()

elif app_mode == "Consultas de inspecciones y AQL":
    st.title("📦 Consultas de inspecciones y AQL")

    # --- SECCIÓN 1: INSPECCIONES DE LLEGADAS ---
    st.subheader("Inspecciones de Llegadas")
    df_llegadas = get_table_as_df('llegadas', order_by="fecha_llegada DESC")
    
    filtro_estado_llegada = st.selectbox("Filtrar por estado de inspección:", ["Todos"] + ESTADOS_INSPECCION, key="filtro_llegadas")
    
    df_filtrado_llegadas = df_llegadas.copy()
    if filtro_estado_llegada != "Todos":
        df_filtrado_llegadas = df_filtrado_llegadas[df_filtrado_llegadas['estado_inspeccion'] == filtro_estado_llegada]

    if 'original_llegadas_df' not in st.session_state or not st.session_state.original_llegadas_df.equals(df_filtrado_llegadas):
        st.session_state.original_llegadas_df = df_filtrado_llegadas.copy()

    def highlight_status(row):
        if row.estado_inspeccion == 'Finalizada':
            style = 'background-color: lightgreen; color: black; font-weight: bold;'
            return [style] * len(row)
        return [''] * len(row)

    st.data_editor(
        df_filtrado_llegadas.style.apply(highlight_status, axis=1),
        column_config={
            "id": None,
            "estado_inspeccion": st.column_config.Column("Estado Inspección", disabled=True),
        },
        use_container_width=True,
        disabled=True 
    )
    
    st.info("El estado de la inspección se actualiza desde la app de Control de Calidad.")
        
    st.divider()

    # --- SECCIÓN 2: INSPECCIÓN AQL ---
    st.subheader("Inspección AQL")
    df_incidencias_activas = get_table_as_df('incidencias_activas')
    df_aql = df_incidencias_activas[df_incidencias_activas['estado'] == 'REVISIÓN AQL'].copy()

    st.metric("Inspecciones AQL pendientes", len(df_aql))
    
    if not df_aql.empty:
        date_cols_aql = ['fecha_apertura', 'fecha_fin', 'fecha_inicio_tarea', 'fecha_prevision_fin']
        for col in date_cols_aql:
            if col in df_aql.columns:
                df_aql[col] = pd.to_datetime(df_aql[col], errors='coerce').dt.date
        st.dataframe(df_aql)
    else:
        st.info("No hay inspecciones AQL pendientes.")

elif app_mode == "Listado de incidencias":
    st.title("📋 Listado de incidencias")
    st.info("Incidencias reportadas desde 'Control de Calidad'. Decide si aceptarlas para seguimiento o rechazarlas.")
    
    df_listado = get_table_as_df("listado_incidencias")

    if df_listado.empty:
        st.success("No hay incidencias pendientes de validar.")
    else:
        st.metric("Incidencias pendientes de validar", len(df_listado))
        
        for _, row in df_listado.iterrows():
            total_nc = int(row.get("total_nc", 0))
            menores  = int(row.get("menores",  0))
            mayores  = int(row.get("mayores",  0))
            criticos = int(row.get("criticos", 0))

            comentarios = (row.get("comentarios_calidad") or "")
            detalles_lista = [c.strip() for c in comentarios.split(";") if c.strip()]
            defect_counts = Counter(detalles_lista)

            header = (
                f"🔍 Ref: {row['referencia']}   "
                f"Desc: {row['descripcion']}   "
                f"Lote: {row['lote'] or 'Sin dato'}"
            )
            with st.expander(header):
                st.markdown(f"**Total no conformidades:** {total_nc}")
                st.markdown(
                    f"**Menores:** {menores}   "
                    f"**Mayores:** {mayores}   "
                    f"**Críticos:** {criticos}"
                )

                if defect_counts:
                    st.markdown("**Defectos detallados:**")
                    for defect, count in defect_counts.items():
                        if count > 1:
                            st.write(f"• {defect} **({count})**")
                        else:
                            st.write(f"• {defect}")

                b1, b2 = st.columns(2)
                if b1.button("✅ Aceptar y abrir seguimiento", key=f"accept_{row['id']}", use_container_width=True):
                    
                    comentarios_formateados = []
                    for defect, count in defect_counts.items():
                        if count > 1:
                            comentarios_formateados.append(f"{defect} ({count})")
                        else:
                            comentarios_formateados.append(defect)
                    comentarios_para_guardar = "; ".join(comentarios_formateados)

                    incidencia_activa = {
                        "pedido":           row.get("pedido", ""),
                        "lote":             row["lote"],
                        "referencia":       row["referencia"],
                        "descripcion":      row["descripcion"],
                        "cantidad_llegada": row.get("cantidad_llegada"),
                        "fecha_apertura":   datetime.now().strftime("%Y-%m-%d"),
                        "comentarios":      comentarios_para_guardar,
                        "estado":           "EN REVISIÓN"
                    }
                    if save_df_to_table(pd.DataFrame([incidencia_activa]), "incidencias_activas"):
                        delete_from_table_by_id("listado_incidencias", row["id"])
                        st.success(f"Incidencia Ref. {row['referencia']} aceptada.")
                        st.rerun()

                if b2.button("❌ Rechazar incidencia", key=f"reject_{row['id']}", use_container_width=True):
                    incidencia_rechazada = {
                        "pedido":              row.get("pedido", ""),
                        "lote":                row["lote"],
                        "referencia":          row["referencia"],
                        "descripcion":         row["descripcion"],
                        "cantidad_llegada":    row.get("cantidad_llegada"),
                        "fecha_llegada":       row.get("fecha_llegada", ""),
                        "comentarios_calidad": row["comentarios_calidad"],
                        "fecha_rechazo":       datetime.now().strftime("%Y-%m-%d")
                    }
                    if save_df_to_table(pd.DataFrame([incidencia_rechazada]), "incidencias_rechazadas"):
                        delete_from_table_by_id("listado_incidencias", row["id"])
                        st.warning(f"Incidencia Ref. {row['referencia']} rechazada.")
                        st.rerun()


elif app_mode == "Seguimiento de incidencias activas":
    st.title("📈 Seguimiento de incidencias activas")
    df_activas = get_table_as_df('incidencias_activas', order_by="id DESC")
    
    def format_estado_with_emoji(estado):
        if estado == "🟢 AQL POSITIVO":
            return f"🟢 AQL POSITIVO"
        elif estado == "🔴 AQL NEGATIVO":
            return f"🔴 AQL NEGATIVO"
        return estado

    with st.expander("Filtrar incidencias", expanded=True):
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            filtro_ref = st.text_input("Filtrar por referencia")
            filtro_estado = st.selectbox("Filtrar por estado", ["Todos"] + ESTADOS)
        with col2:
            filtro_tipo = st.selectbox("Filtrar por tipo", ["Todos"] + TIPOS_INCIDENCIA)
            filtro_procedencia = st.selectbox("Filtrar por procedencia", ["Todos"] + PROCEDENCIAS)
        with col3:
            filtro_categoria = st.selectbox("Filtrar por categoría", ["Todos"] + CATEGORIAS)
            filtro_origen = st.selectbox("Filtrar por origen", ["Todos"] + ORIGENES_INCIDENCIA)
        with col4:
            filtro_bloqueo = st.selectbox("Filtrar por bloqueo", ["Todos"] + ESTADOS_BLOQUEO)

    df_filtrado = df_activas.copy()
    if filtro_ref:
        df_filtrado = df_filtrado[df_filtrado['referencia'].astype(str).str.contains(filtro_ref, case=False)]
    if filtro_estado != "Todos":
        estado_sin_emoji = filtro_estado.replace('🟢 ', '').replace('🔴 ', '')
        df_filtrado = df_filtrado[df_filtrado['estado'] == estado_sin_emoji]
    if filtro_tipo != "Todos":
        df_filtrado = df_filtrado[df_filtrado['tipo_incidencia'] == filtro_tipo]
    if filtro_procedencia != "Todos":
        df_filtrado = df_filtrado[df_filtrado['procedencia'] == filtro_procedencia]
    if filtro_categoria != "Todos":
        df_filtrado = df_filtrado[df_filtrado['categoria'] == filtro_categoria]
    if filtro_origen != "Todos":
        df_filtrado = df_filtrado[df_filtrado['origen_incidencia'] == filtro_origen]
    if filtro_bloqueo != "Todos":
        df_filtrado = df_filtrado[df_filtrado['bloqueo_referencias'] == filtro_bloqueo]

    date_columns_to_convert = ["fecha_apertura", "fecha_fin", "fecha_inicio_tarea", "fecha_prevision_fin"]
    for col in date_columns_to_convert:
        if col in df_filtrado.columns:
            df_filtrado[col] = pd.to_datetime(df_filtrado[col], errors='coerce')

    df_display = df_filtrado.copy()
    if 'estado' in df_display.columns:
        df_display['estado'] = df_display['estado'].apply(format_estado_with_emoji)
    
    # Insertamos la columna para seleccionar filas a borrar
    df_display.insert(0, "Borrar", False)
    
    st.info("Puedes editar datos en la tabla, o seleccionar incidencias y usar el botón de eliminar.")
    
    if 'original_df' not in st.session_state or not st.session_state.original_df.equals(df_filtrado):
        st.session_state.original_df = df_filtrado.copy()
    
    column_order = (
        "Borrar", "Nº", "estado", "referencia", "categoria", "descripcion", "lote", "fecha_apertura", 
        "procedencia", "origen_incidencia", "tipo_incidencia", "prioridad", "fecha_fin", 
        "pedido", "marca", "picking", "cantidad_llegada", "middle", "comentarios", "acciones",
        "bloqueo_referencias", "fecha_inicio_tarea", "fecha_prevision_fin",
        "coste_material_y_personal", "recuperado_proveedor", "responsable_calidad",
        "bultos", "master", "piezas_a_revisar", "masters_diferentes"
    )

    column_config = {
        "Borrar": st.column_config.CheckboxColumn("Borrar", default=False),
        "id": None, "Nº": st.column_config.Column("Nº", disabled=True),
        "estado": st.column_config.SelectboxColumn("ESTADO", options=ESTADOS),
        "referencia": st.column_config.Column("REF"),
        "categoria": st.column_config.SelectboxColumn("CATEGORIA", options=CATEGORIAS),
        "descripcion": st.column_config.Column("DESCRIPCIÓN"), "lote": st.column_config.Column("LOTE"),
        "fecha_apertura": st.column_config.DateColumn("FECHA INCIDENCIA", format="YYYY-MM-DD"),
        "procedencia": st.column_config.SelectboxColumn("PROCEDENCIA", options=PROCEDENCIAS),
        "origen_incidencia": st.column_config.SelectboxColumn("ORIGEN DE LA INCIDENCIA", options=ORIGENES_INCIDENCIA),
        "tipo_incidencia": st.column_config.SelectboxColumn("TIPO INCIDENCIA", options=TIPOS_INCIDENCIA),
        "prioridad": st.column_config.Column("PRIORIDAD"),
        "fecha_fin": st.column_config.DateColumn("FECHA FIN", format="YYYY-MM-DD"),
        "pedido": st.column_config.Column("PEDIDO/LOTE"), "marca": st.column_config.Column("MARCA"),
        "picking": st.column_config.Column("PICKING"), "cantidad_llegada": st.column_config.NumberColumn("QTY"),
        "middle": st.column_config.NumberColumn("MIDDLE"),
        "comentarios": st.column_config.Column("DESCRIPCIÓN INCIDENCIA", width="large"),
        "acciones": st.column_config.Column("ACCIONES", width="large"),
        "bloqueo_referencias": st.column_config.SelectboxColumn("BLOQUEO REFERENCIAS", options=ESTADOS_BLOQUEO),
        "fecha_inicio_tarea": st.column_config.DateColumn("FECHA INICIO TAREA", format="YYYY-MM-DD"),
        "fecha_prevision_fin": st.column_config.DateColumn("FECHA PREVISIÓN FIN", format="YYYY-MM-DD"),
        "coste_material_y_personal": st.column_config.NumberColumn("COSTE MATERIAL Y PERSONAL", format="%.2f €"),
        "recuperado_proveedor": st.column_config.NumberColumn("RECUPERADO PROVEEDOR", format="%.2f €"),
        "responsable_calidad": st.column_config.Column("RESPONSABLE CALIDAD"),
        "bultos": st.column_config.NumberColumn("BULTOS"), "master": st.column_config.NumberColumn("MASTER"),
        "piezas_a_revisar": st.column_config.NumberColumn("PIEZAS A REVISAR"),
        "masters_diferentes": st.column_config.Column("MASTERS DIFERENTES")
    }

    edited_df = st.data_editor(
        df_display.reset_index(drop=True).rename_axis("Nº").reset_index(),
        key="data_editor_incidencias", column_order=column_order,
        column_config=column_config, use_container_width=True, num_rows="dynamic"
    )

    # Lógica para el borrado de incidencias
    col_save, col_delete = st.columns(2)
    with col_save:
        if st.button("Guardar cambios en celdas", use_container_width=True):
            edited_df_processed = edited_df.drop(columns=['Nº', 'Borrar'])
            
            if 'estado' in edited_df_processed.columns:
                edited_df_processed['estado'] = edited_df_processed['estado'].str.replace('🟢 ', '').str.replace('🔴 ', '')

            original_df_reindexed = st.session_state.original_df.set_index('id')
            edited_df_reindexed = edited_df_processed.set_index('id')
            
            original_aligned, edited_aligned = original_df_reindexed.align(edited_df_reindexed, join='inner', axis=0)
            original_aligned.fillna('', inplace=True)
            edited_aligned.fillna('', inplace=True)

            diff_mask = original_aligned.ne(edited_aligned)
            changed_rows = diff_mask.any(axis=1)
            
            if not changed_rows.any():
                st.info("No se detectaron cambios para guardar.")
            else:
                updates_count = 0
                for record_id in changed_rows[changed_rows].index:
                    update_data = edited_df_reindexed.loc[record_id].to_dict()
                    update_data_cleaned = {k: v for k, v in update_data.items() if pd.notna(v)}

                    if update_data_cleaned:
                        if update_record('incidencias_activas', record_id, update_data_cleaned):
                            updates_count += 1
                
                if updates_count > 0:
                    st.success(f"Se guardaron con éxito los cambios en **{updates_count}** registros.")
                    del st.session_state.original_df
                    st.rerun()
                else:
                    st.warning("No se pudieron guardar los cambios.")
    
    with col_delete:
        if st.button("🗑️ Eliminar incidencias seleccionadas", type="primary", use_container_width=True):
            filas_a_borrar = edited_df[edited_df["Borrar"] == True]
            if not filas_a_borrar.empty:
                st.session_state.ids_para_borrar = filas_a_borrar['id'].tolist()
                st.rerun()
            else:
                st.warning("Por favor, selecciona al menos una incidencia para eliminar.")

    # Diálogo de confirmación de borrado
    if 'ids_para_borrar' in st.session_state and st.session_state.ids_para_borrar:
        num_incidencias = len(st.session_state.ids_para_borrar)
        st.warning(f"**¿Estás seguro de que quieres eliminar {num_incidencias} incidencia(s)?** Esta acción no se puede deshacer.")
        
        confirm_col, cancel_col = st.columns(2)
        with confirm_col:
            if st.button("Sí, eliminar", use_container_width=True):
                borradas_count = 0
                for record_id in st.session_state.ids_para_borrar:
                    if delete_from_table_by_id('incidencias_activas', record_id):
                        borradas_count += 1
                
                st.success(f"Se han eliminado {borradas_count} incidencias.")
                del st.session_state.ids_para_borrar
                st.cache_data.clear()
                st.rerun()
        
        with cancel_col:
            if st.button("Cancelar", use_container_width=True):
                del st.session_state.ids_para_borrar
                st.rerun()

    # Expander de importación
    with st.expander("Importar histórico de incidencias desde Excel"):
        st.warning("Esta acción es para la carga inicial de datos. Los registros duplicados serán ignorados.")
        
        historic_file = st.file_uploader("Sube el archivo 'INCIDENCIAS CALIDAD 2025 (1).xlsx'", type=['xlsx', 'csv'])
        
        if historic_file:
            try:
                df_historic = pd.read_excel(historic_file, sheet_name='incidencias')

                column_map = {
                    'RESPONSABLE CALIDAD': 'responsable_calidad', 'ESTADO': 'estado', 'PROCEDENCIA': 'procedencia',
                    'CATEGORIA': 'categoria', 'ORIGEN DE LA INCIDENCIA': 'origen_incidencia', 'TIPO INCIDENCIA': 'tipo_incidencia',
                    'PRIORIDAD': 'prioridad', 'FECHA INCIDENCIA': 'fecha_apertura', 'FECHA FIN': 'fecha_fin',
                    'PEDIDO/LOTE': 'pedido', 'MARCA': 'marca', 'REF': 'referencia', 'DESCRIPCIÓN': 'descripcion',
                    'PICKING': 'picking', 'QTY': 'cantidad_llegada', 'BULTOS': 'bultos', 'MASTER': 'master',
                    'MIDDLE': 'middle', 'PIEZAS A REVISAR': 'piezas_a_revisar', 'MASTERS DIFERENTES': 'masters_diferentes',
                    'DESCRIPCIÓN INCIDENCIA': 'comentarios', 'ACCIONES': 'acciones', 'BLOQUEO REFERENCIAS': 'bloqueo_referencias',
                    'FECHA INICIO TAREA': 'fecha_inicio_tarea', 'FECHA PREVISIÓN FIN': 'fecha_prevision_fin',
                    'COSTE MATERIAL Y PERSONAL': 'coste_material_y_personal', 'RECUPERADO PROVEEDOR': 'recuperado_proveedor'
                }
                df_historic.rename(columns=column_map, inplace=True)
                
                if 'pedido' in df_historic.columns:
                    df_historic['lote'] = df_historic['pedido'].astype(str).str[-8:]

                final_cols = [col for col in column_map.values() if col in df_historic.columns]
                if 'lote' in df_historic.columns:
                    final_cols.append('lote')

                df_to_import = df_historic[final_cols].copy()
                
                for col in ['estado', 'procedencia', 'categoria', 'origen_incidencia', 'tipo_incidencia', 'bloqueo_referencias']:
                        if col in df_to_import.columns:
                                df_to_import[col] = df_to_import[col].astype(str).str.strip()
                
                subset_cols_check = ['referencia', 'lote']
                subset_cols_check = [col for col in subset_cols_check if col in df_to_import.columns]

                if save_df_to_table(df_to_import, 'incidencias_activas', check_duplicates=True, subset_cols=subset_cols_check):
                    st.success("Datos históricos importados con éxito. Los duplicados han sido ignorados.")
                    st.rerun()

            except Exception as e:
                st.error(f"Error al procesar el archivo histórico: {e}")
                st.error("Asegúrate de que el archivo es correcto y contiene una hoja llamada 'incidencias'.")

elif app_mode == "Consultar llegadas ignoradas":
    st.title("🔍 Consultar llegadas ignoradas")
    st.info("Productos que fueron marcados como 'No es necesario revisar'.")
    
    df_ignoradas = get_table_as_df('llegadas_ignoradas', order_by="fecha_llegada DESC")
    
    if df_ignoradas.empty:
        st.success("No hay llegadas ignoradas.")
    else:
        st.metric("Total de llegadas ignoradas", len(df_ignoradas))
        st.dataframe(df_ignoradas)

elif app_mode == "Consultar incidencias rechazadas":
    st.title("❌ Consultar incidencias rechazadas")
    st.info("Incidencias que fueron rechazadas. Desde aquí puedes reconsiderar la decisión y moverlas a seguimiento.")
    df_rechazadas = get_table_as_df('incidencias_rechazadas')
    if df_rechazadas.empty:
        st.success("No hay incidencias rechazadas.")
    else:
        st.metric("Total de incidencias rechazadas", len(df_rechazadas))
        for index, row in df_rechazadas.iterrows():
            with st.container(border=True):
                col1, col2 = st.columns([4, 1])
                with col1:
                    st.markdown(f"**Ref:** {row['referencia']} | **Lote:** {row['lote']} | **Pedido:** {row['pedido']}")
                    st.write(f"**Comentarios de Calidad:** {row['comentarios_calidad']}")
                if col2.button("↩️ Reconsiderar y mover a seguimiento", key=f"reconsider_{row['id']}", use_container_width=True):
                    incidencia_activa = {
                        'pedido': row['pedido'], 'lote': row['lote'], 'referencia': row['referencia'], 'descripcion': row['descripcion'],
                        'cantidad_llegada': row['cantidad_llegada'], 'fecha_apertura': datetime.now().strftime('%Y-%m-%d'),
                        'comentarios': row['comentarios_calidad'], 'estado': 'EN REVISIÓN'
                    }
                    if save_df_to_table(pd.DataFrame([incidencia_activa]), 'incidencias_activas'):
                        delete_from_table_by_id('incidencias_rechazadas', row['id'])
                        st.success(f"Incidencia para Ref. {row['referencia']} movida a seguimiento.")
                        st.rerun()

# ==============================================================================
# ======================== BLOQUE DE INDICADORES ===================
# ==============================================================================
elif app_mode == "Indicadores":
    st.title("📊 Indicadores de Incidencias")
    import plotly.express as px

    df_indicadores = get_table_as_df('incidencias_activas')

    if df_indicadores.empty:
        st.warning("No hay datos de incidencias activas para mostrar indicadores.")
        st.stop()

    st.sidebar.header("Filtros de Indicadores")
    df_indicadores["fecha_apertura"] = pd.to_datetime(df_indicadores["fecha_apertura"], errors="coerce")
    df_valid_dates = df_indicadores.dropna(subset=["fecha_apertura"])

    if df_valid_dates.empty:
        st.error("No hay incidencias con fechas de apertura válidas para filtrar.")
        st.stop()
        
    min_date = df_valid_dates["fecha_apertura"].min().date()
    max_date = df_valid_dates["fecha_apertura"].max().date()
    
    fecha_inicio = st.sidebar.date_input("Fecha de inicio", min_date, min_value=min_date, max_value=max_date)
    fecha_fin = st.sidebar.date_input("Fecha de fin", max_date, min_value=min_date, max_value=max_date)

    if fecha_inicio > fecha_fin:
        st.sidebar.error("La fecha de inicio no puede ser posterior a la fecha de fin.")
        st.stop()

    mask = (df_valid_dates['fecha_apertura'].dt.date >= fecha_inicio) & (df_valid_dates['fecha_apertura'].dt.date <= fecha_fin)
    df_filtrado = df_valid_dates.loc[mask]

    st.metric(
        label=f"Total de Incidencias ({fecha_inicio.strftime('%d/%m/%Y')} - {fecha_fin.strftime('%d/%m/%Y')})",
        value=f"{len(df_filtrado):,}"
    )
    st.divider()

    if df_filtrado.empty:
        st.info("No se encontraron incidencias en el rango de fechas seleccionado.")
    else:
        st.subheader("Distribución de Incidencias")
        
        cols_to_plot = [
            ("Procedencia", "procedencia"), ("Categoría", "categoria"),
            ("Tipo de Incidencia", "tipo_incidencia"), ("Estado Actual", "estado")
        ]
        
        datos_resumen = {}
        
        col1, col2 = st.columns(2)
        columns_list = [col1, col2]
        
        for i, (title, col_name) in enumerate(cols_to_plot):
            if col_name in df_filtrado.columns:
                with columns_list[i % 2]:
                    df_chart_data = df_filtrado.dropna(subset=[col_name])
                    df_chart_data = df_chart_data[df_chart_data[col_name].astype(str) != 'nan']

                    if df_chart_data.empty:
                        st.info(f"No hay datos válidos para mostrar en '{title}'.")
                        continue

                    resumen = df_chart_data[col_name].value_counts().reset_index()
                    resumen.columns = [title, "Cantidad"]
                    datos_resumen[title] = resumen
                    
                    fig_plotly = px.pie(resumen, names=title, values='Cantidad', 
                                        title=f"Distribución por {title}", hole=0.3)
                    fig_plotly.update_traces(textposition='inside', textinfo='percent+label')
                    fig_plotly.update_layout(showlegend=False)
                    st.plotly_chart(fig_plotly, use_container_width=True)

        st.divider()
        st.subheader("Descargar Informes")

        def generar_pdf_indicadores(resumen_data, f_inicio, f_fin):
            pdf_buffer = io.BytesIO()
            doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(A4), rightMargin=0.5*inch, leftMargin=0.5*inch, topMargin=0.4*inch, bottomMargin=0.4*inch)
            styles = getSampleStyleSheet()
            elements = []

            elements.append(Paragraph("Informe de Indicadores de Incidencias", styles['h1']))
            elements.append(Paragraph(f"Periodo del informe: {f_inicio.strftime('%d-%m-%Y')} al {f_fin.strftime('%d-%m-%Y')}", styles['Normal']))
            elements.append(Spacer(1, 0.2 * inch))

            charts_row, tables_row = [], []

            for title, resumen in resumen_data.items():
                if resumen.empty: continue
                
                plt.style.use('seaborn-v0_8-whitegrid')
                fig_mpl, ax = plt.subplots(figsize=(4, 3), dpi=200)
                
                labels, sizes = resumen[title], resumen["Cantidad"]
                total = float(sum(sizes))
                legend_labels = [f'{l} ({s/total*100:.1f}%)' for l, s in zip(labels, sizes)]

                wedges, _ = ax.pie(sizes, startangle=90, textprops=dict(color="w"))
                ax.legend(wedges, legend_labels, title=title, loc="center left", bbox_to_anchor=(1, 0.5), fontsize='xx-small')
                
                img_buffer = io.BytesIO()
                fig_mpl.savefig(img_buffer, format='png', bbox_inches='tight')
                plt.close(fig_mpl)
                img_buffer.seek(0)
                charts_row.append(ReportLabImage(img_buffer, width=2.5*inch, height=1.88*inch))

                data = [resumen.columns.tolist()] + resumen.values.tolist()
                data_table = Table(data, colWidths=[1.8*inch, 0.6*inch])
                data_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.darkblue), ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                    ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                    ('GRID', (0, 0), (-1, -1), 1, colors.black), ('FONTSIZE', (0, 0), (-1, -1), 7),
                    ('TOPPADDING', (0, 0), (-1, -1), 2), ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ]))
                tables_row.append(data_table)

            while len(charts_row) < 4: charts_row.append("")
            while len(tables_row) < 4: tables_row.append("")

            if charts_row and tables_row:
                main_table = Table([charts_row, tables_row], colWidths=[2.6*inch]*4, rowHeights=[2.5*inch, None])
                main_table.setStyle(TableStyle([
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'), ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ]))
                elements.append(main_table)

            doc.build(elements)
            pdf_buffer.seek(0)
            return pdf_buffer

        col_dl1, col_dl2 = st.columns(2)
        with col_dl1:
            excel_buffer = io.BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='xlsxwriter') as writer:
                df_excel_export = df_filtrado.copy()
                for col in df_excel_export.select_dtypes(include=['datetime64[ns]']).columns:
                    df_excel_export[col] = df_excel_export[col].dt.strftime('%Y-%m-%d')
                df_excel_export.to_excel(writer, sheet_name='Incidencias_Filtradas', index=False)
                
                for sheet_name, df_resumen in datos_resumen.items():
                    safe_sheet_name = re.sub(r'[\\/*?:"<>|]', "", sheet_name)[:31]
                    df_resumen.to_excel(writer, sheet_name=safe_sheet_name, index=False)
            
            st.download_button(
                label="📥 Descargar informe Excel", data=excel_buffer.getvalue(),
                file_name=f"Informe_Indicadores_{fecha_inicio}_a_{fecha_fin}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        with col_dl2:
            pdf_bytes = generar_pdf_indicadores(datos_resumen, fecha_inicio, fecha_fin)
            st.download_button(
                label="📄 Descargar informe PDF", data=pdf_bytes,
                file_name=f"Informe_Indicadores_{fecha_inicio}_a_{fecha_fin}.pdf",
                mime="application/pdf", use_container_width=True
            )

elif app_mode == "Gestión de datos maestros":
    st.title("🗃️ Gestión de Datos Maestros")
    st.info("Carga y actualiza los archivos Excel en la base de datos PostgreSQL para mejorar el rendimiento.")
    
    # Mostrar estado actual
    col1, col2 = st.columns(2)
    
    engine = get_db_engine()
    if engine:
        with col1:
            try:
                count_alehop = pd.read_sql_query("SELECT COUNT(DISTINCT product_id) as total FROM alehop_data", engine)
                fecha_alehop = pd.read_sql_query("SELECT MAX(fecha_actualizacion) as fecha FROM alehop_data", engine)
                
                st.metric("Referencias en Ale-hop Data", f"{count_alehop['total'].iloc[0]:,}")
                if fecha_alehop['fecha'].iloc[0]:
                    st.caption(f"Última actualización: {fecha_alehop['fecha'].iloc[0]}")
            except:
                st.metric("Referencias en Ale-hop Data", "0")
                st.caption("Sin datos cargados")
        
        with col2:
            try:
                count_bbdd = pd.read_sql_query("SELECT COUNT(*) as total FROM bbdd_articulos", engine)
                fecha_bbdd = pd.read_sql_query("SELECT MAX(fecha_actualizacion) as fecha FROM bbdd_articulos", engine)
                
                st.metric("Referencias en BBDD Artículos", f"{count_bbdd['total'].iloc[0]:,}")
                if fecha_bbdd['fecha'].iloc[0]:
                    st.caption(f"Última actualización: {fecha_bbdd['fecha'].iloc[0]}")
            except:
                st.metric("Referencias en BBDD Artículos", "0")
                st.caption("Sin datos cargados")
    
    st.divider()
    
    # Tabs para cada archivo
    tab1, tab2 = st.tabs(["📊 Ale-hop Data", "📦 BBDD Artículos"])
    
    with tab1:
        st.subheader("Cargar Ale-hop Data fecha y cat.xlsx")
        
        alehop_file = st.file_uploader(
            "Sube el archivo Excel con datos de ventas y mermas",
            type=['xlsx'],
            key="alehop_uploader"
        )
        
        if alehop_file:
            try:
                df_alehop = pd.read_excel(alehop_file, engine='openpyxl')
                df_alehop.columns = [str(c).upper().strip() for c in df_alehop.columns]
                
                st.success(f"✅ Archivo leído correctamente: {len(df_alehop):,} filas")
                
                # Mostrar preview
                with st.expander("Vista previa de los datos"):
                    st.dataframe(df_alehop.head(10))
                
                # Verificar columnas requeridas
                required_cols = {'PRODUCT ID', 'AÑO MES NO', 'PCS SOLD', 'BROKEN PCS'}
                if not required_cols.issubset(df_alehop.columns):
                    st.error(f"⚠️ Faltan columnas requeridas: {required_cols - set(df_alehop.columns)}")
                else:
                    if st.button("💾 Cargar datos en PostgreSQL", key="load_alehop"):
                        with st.spinner("Cargando datos..."):
                            # Preparar datos
                            df_to_load = df_alehop.copy()
                            df_to_load.rename(columns={
                                'PRODUCT ID': 'product_id',
                                'AÑO MES NO': 'ano_mes_no',
                                'PCS SOLD': 'pcs_sold',
                                'BROKEN PCS': 'broken_pcs'
                            }, inplace=True)
                            
                            # Convertir fecha
                            df_to_load['ano_mes_no'] = pd.to_datetime(df_to_load['ano_mes_no'], errors='coerce')
                            
                            # Añadir categoría si existe
                            if 'CATEGORIA' in df_alehop.columns:
                                df_to_load['categoria'] = df_alehop['CATEGORIA']
                            
                            # Seleccionar columnas válidas
                            valid_cols = ['product_id', 'ano_mes_no', 'pcs_sold', 'broken_pcs', 'categoria']
                            valid_cols = [col for col in valid_cols if col in df_to_load.columns]
                            df_to_load = df_to_load[valid_cols]
                            
                            # Eliminar filas con product_id o ano_mes_no nulos
                            df_to_load = df_to_load.dropna(subset=['product_id', 'ano_mes_no'])
                            
                            # Limpiar tabla existente
                            with engine.connect() as conn:
                                conn.execute(text("TRUNCATE TABLE alehop_data"))
                                conn.commit()
                            
                            # Cargar datos
                            df_to_load.to_sql('alehop_data', engine, if_exists='append', index=False)
                            
                            st.success(f"✅ Se cargaron {len(df_to_load):,} registros en la base de datos")
                            st.cache_data.clear()  # Limpiar caché para actualizar datos
                            st.rerun()
                            
            except Exception as e:
                st.error(f"Error al procesar el archivo: {e}")
    
    with tab2:
        st.subheader("Cargar BBDD ARTICULOS.xlsx")
        
        bbdd_file = st.file_uploader(
            "Sube el archivo Excel con el catálogo de artículos",
            type=['xlsx'],
            key="bbdd_uploader"
        )
        
        if bbdd_file:
            try:
                df_bbdd = pd.read_excel(bbdd_file, engine='openpyxl', header=None)
                
                st.success(f"✅ Archivo leído correctamente: {len(df_bbdd):,} filas")
                
                # Mostrar preview
                with st.expander("Vista previa de los datos"):
                    st.dataframe(df_bbdd.head(10))
                
                # Verificar que tiene al menos 2 columnas
                if df_bbdd.shape[1] < 2:
                    st.error("⚠️ El archivo debe tener al menos 2 columnas (la columna B contiene las referencias)")
                else:
                    st.info("Se utilizará la columna B (índice 1) para las referencias")
                    
                    if st.button("💾 Cargar datos en PostgreSQL", key="load_bbdd"):
                        with st.spinner("Cargando datos..."):
                            # Extraer referencias de la columna B
                            referencias = df_bbdd.iloc[:, 1].dropna().astype(str).str.strip()
                            referencias = referencias[referencias != '']  # Eliminar vacíos
                            
                            # Crear DataFrame
                            df_to_load = pd.DataFrame({'referencia': referencias})
                            
                            # Limpiar tabla existente
                            with engine.connect() as conn:
                                conn.execute(text("TRUNCATE TABLE bbdd_articulos"))
                                conn.commit()
                            
                            # Cargar datos
                            df_to_load.to_sql('bbdd_articulos', engine, if_exists='append', index=False)
                            
                            st.success(f"✅ Se cargaron {len(df_to_load):,} referencias en la base de datos")
                            st.cache_data.clear()  # Limpiar caché para actualizar datos
                            st.rerun()
                            
            except Exception as e:
                st.error(f"Error al procesar el archivo: {e}")
    
    st.divider()
    
    # Sección de configuración
    st.subheader("⚙️ Configuración")
    
    current_mode = "Base de datos" if USE_DB_FOR_EXCEL_DATA else "Archivos Excel"
    st.info(f"**Modo actual de lectura de datos:** {current_mode}")
    
    st.markdown("""
    ### 📝 Instrucciones:
    
    1. **Carga inicial**: Sube ambos archivos Excel para poblar la base de datos
    2. **Actualizaciones**: Vuelve a cargar los archivos cuando necesites actualizar los datos
    3. **Variable de entorno**: Configura `USE_DB_FOR_EXCEL_DATA=True` para usar datos de PostgreSQL
    
    ### ✅ Ventajas de usar PostgreSQL:
    - Mayor velocidad de procesamiento
    - No necesitas acceso a rutas de red
    - Mejor para entornos cloud (Coolify)
    - Datos centralizados y versionados
    """)
    
    # Opción para descargar datos actuales
    if st.checkbox("🔍 Ver estadísticas de datos"):
        if engine:
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("**Estadísticas Ale-hop Data:**")
                try:
                    stats = pd.read_sql_query("""
                        SELECT 
                            COUNT(DISTINCT product_id) as productos_unicos,
                            COUNT(*) as total_registros,
                            MIN(ano_mes_no) as fecha_min,
                            MAX(ano_mes_no) as fecha_max
                        FROM alehop_data
                    """, engine)
                    
                    for col, val in stats.iloc[0].items():
                        st.write(f"- {col}: {val}")
                except:
                    st.write("Sin datos")
            
            with col2:
                st.markdown("**Estadísticas BBDD Artículos:**")
                try:
                    stats = pd.read_sql_query("""
                        SELECT 
                            COUNT(*) as total_referencias,
                            MIN(fecha_actualizacion) as primera_carga,
                            MAX(fecha_actualizacion) as ultima_actualizacion
                        FROM bbdd_articulos
                    """, engine)
                    
                    for col, val in stats.iloc[0].items():
                        st.write(f"- {col}: {val}")
                except:
                    st.write("Sin datos")

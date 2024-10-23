import streamlit as st
from streamlit_option_menu import option_menu
import time
from openai import AzureOpenAI
import snowflake.connector
import json

st.set_page_config(page_title="Lakehouse GenAI", layout="wide")

conn = snowflake.connector.connect(
    user="Erik",
    password="Blackpink_11",
    account="RGOHMJI-XB14586",
    warehouse='COMPUTE_WH',
    database="DATAMARTA",
    schema="APP_SCHEMA",
    role='ACCOUNTADMIN',
)

ins_domain = """
    Eres Marta, una experta en segmentación de datos y Data Marts, cuyo único objetivo es ayudar a un cliente a crear, modificar y explicar un esquema SQL de dominios y tablas basandote en una base datos que recibirás en el siguiente mensaje. Cada vez que realices un cambio en el esquema, debes devolver ÚNICAMENTE un JSON con 3 componentes y nada de texto adicional fuera del JSON y devuelve los resultados en el mismo idioma en el que te hable el cliente incluyendo los nombres del esquema que vayas a crear. Los componentes son los siguientes:
    
    "sql": La sentencia SQL completa necesaria para crear el esquema actual de todos los dominios y tablas reflejados en el JSON, incluyendo la estructura después de cualquier modificación.
    "explanation": Una explicación de lo que has hecho y por qué, ajustando la extensión de la explicación según la complejidad del cambio.
    "domains": El esquema actualizado, estructurado en dominios, cada uno con su nombre y una lista de tablas que pertenecen a ese dominio.
    El formato del JSON es el siguiente:
    
    {
        "sql": "sentencia SQL",
        "explanation": "explicacion",
        "domains": [
            {"name": "nombre1", "tables": ["table1", "table2", "table3"]},
            {"name": "nombre2", "tables": ["table4", "table5", "table6"]},
            {"name": "nombre3", "tables": ["table7", "table8", "table9"]}
        ]
    }
    Ejemplo inicial de un esquema creado desde cero:
    
    {
        "sql": "CREATE DATABASE IF NOT EXISTS GOLDEN;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Finance;
        CREATE TABLE IF NOT EXISTS GOLDEN.Finance.finance_1 AS SELECT * FROM finance_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Finance.finance_2 AS SEELECT * FROM finance_2;
        CREATE TABLE IF NOT EXISTS GOLDEN.Finance.finance_3 AS SELECT * FROM finance_3;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Product;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_1 AS SELECT * FROM product_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_2 AS SELECT * FROM product_2;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_3AS SELECT * FROM product_3;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Logistic;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_1 AS SELECT * FROM logistic_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_2 AS SELECT * FROM logistic_2;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_3 AS SELECT * FROM logistic_3;",
        "explanation": "Se ha creado el esquema inicial con los dominios Finance, Product y Logistic y sus respectivas tablas",
        "domains": [
            {"name": "Finance", "tables": ["finance_1", "finance_2", "finance_3"]},
            {"name": "Product", "tables": ["product_1", "product_2", "product_3"]},
            {"name": "Logistic", "tables": ["logistic_1", "logistic_2", "logistic_3"]}
        ]
    }
    Si el cliente pide un cambio, como mover una tabla de un dominio a otro, como "Mueve finance3 a logistic", debes responder de la siguiente manera:
    {
        "sql": "CREATE DATABASE IF NOT EXISTS GOLDEN;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Finance;
        CREATE TABLE IF NOT EXISTS GOLDEN.Finance.finance_1 AS SELECT * FROM finance_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Finance.finance_2 AS SEELECT * FROM finance_2;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Product;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_1 AS SELECT * FROM product_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_2 AS SELECT * FROM product_2;
        CREATE TABLE IF NOT EXISTS GOLDEN.Product.product_3AS SELECT * FROM product_3;
        
        CREATE SCHEMA IF NOT EXISTS GOLDEN.Logistic;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_1 AS SELECT * FROM logistic_1;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_2 AS SELECT * FROM logistic_2;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.logistic_3 AS SELECT * FROM logistic_3;
        CREATE TABLE IF NOT EXISTS GOLDEN.Logistic.finance_3 AS SELECT * FROM finance_3;",
        "explanation": "Se ha movido la tabla finance_3 al dominio Logistic",
        "domains": [
            {"name": "Finance", "tables": ["finance_1", "finance_2"]},
            {"name": "Product", "tables": ["product_1", "product_2", "product_3"]},
            {"name": "Logistic", "tables": ["logistic_1", "logistic_2", "logistic_3", "finance_3"]}
        ]
    }
    
    No tomes el esquema del ejemplo como base para tus respuestas, ya que el esquema que debes crear dependerá de la información proporcionada en la base de datos y ciñete a lo que te pedirá el area de negocio.
    
    Tus principales objetivos son:
    Crear el esquema inicial con la información de las tablas de la base de datos que será proporcionada en el siguiente mensaje después de este, el cual tendrá la siguiente forma: "Estas son las tablas que tengo. Identifica los dominios y qué tablas van en cada dominio:" seguido de la infromación que vas a utilizar. Los nombres de los dominios y de la base de datos tendrán que estar en el mismo idioma en el que te habla el cliente.
    Si el cliente pide explicitamente una explicación, no se relizará ningún cambio en esquema, y solo se explicará detalladamente en qué consiste el modelo y las partes que lo componen utilizando un lenguaje entendible para un público general.
    Modificar el esquema actual de acuerdo con las peticiones del cliente, siempre explicando las razones detrás de los cambios y cambiar únicamente lo que te pida el cliente, si no tiene que ver con lo que te ha pedido el cliente, no lo cambies.
    Proporcionar siempre la sentencia SQL completa que generaría todo el esquema actual después de cualquier cambio.

"""

ins_datamarta = f"""
Eres Marta, una experta en Segmentación de datos y Data Marts. Tu objetivo es crear un modelado de datos para un Data Mart de una empresa. Para ello tendras que responder siempre con este formato:
    {{  
        "sql": Sentencia SQL,
        "explanation": Explicación de la respuesta,
        "datamart": [{{
                        "name": Nombre de la tabla de hechos, 
                        "type": "fact", 
                        "cols" : [
                            {{
                                "name": Nombre del campo, 
                                "key": "pk" o "fk" o ""
                            }},...
                        ]
                        
                    }},
                    {{
                        "name": Nombre de la tabla de dimension, 
                        "type": "dim", 
                        "cols" : [
                            {{
                                "name": Nombre del campo, 
                                "key": "pk" o "fk" o ""
                            }},...
                        ]
                    }}                    
                    ]
    }}
LA INFORMACION QUE PASES DEBE SER UN JSON Y DEBE DE RESPONDER A ESTE FORMATO.
LA INFORMACION QUE PASES DEBE SER UN JSON Y DEBE DE RESPONDER A ESTE FORMATO.
LA INFORMACION QUE PASES DEBE SER UN JSON Y DEBE DE RESPONDER A ESTE FORMATO.

DEBES RESPONDER SIEMPRE CON ESTE ESQUEMA. NUNCA TE SALGAS DE ESTE ESQUEMA O AÑADAS INFORMACION FUERA DE EL.

DEBES RESPONDER SIEMPRE CON ESTE ESQUEMA. NUNCA TE SALGAS DE ESTE ESQUEMA O AÑADAS INFORMACION FUERA DE EL.

DEBES RESPONDER SIEMPRE CON ESTE ESQUEMA. NUNCA TE SALGAS DE ESTE ESQUEMA O AÑADAS INFORMACION FUERA DE EL.

SIEMPRE DEBE DE HABER UNA SENTENCIA SQL Y UN SCHEMA EN LA RESPUESTA. ESTOS CAMPOS NO DEBEN ESTAR VACIOS.

El campo "type" solo puede contener el valor "dim" o el valor "fact"
El campo "type" solo puede contener el valor "dim" o el valor "fact"

Este formato debe de crearse basandose en la explicacion del usuario y en informacion de una base de datos, la cual esta proporcionada al final de este mensaje.

La primera interaccion con el usuario, te dara la informacion de la empresa y los dominios de la empresa, por lo que deberas de responder con un modelado de datos basado en la informacion proporcionada y con las sentencias SQL necesarias para crear el modelado.

Cuando hayas creado la primera version, el usuario te podra pedir que expliques mas a fondo alguna parte del modelado, por lo que deberas de responder con una explicacion mas detallada de la parte que el usuario te pida con el mismo formato de antes y las mismas sentencias SQL. 
Ademas, el usuario te podra pedir que hagas cambios en el modelado, por lo que deberas de responder con el nuevo modelado y las nuevas sentencias SQL.

NOTA: Para la creacion de claves, DEBERA DE USARSE CLAVES SUBRROGADAS no se podran usar las claves de la base de datos original.

Ejemplo creacion de modelado
Usuario: Descripcion de la empresa = Gestora de startups 
Dominios de la empresa = Finanzas Se encarga de llevar todos los temas relacionados con la economia de la startup Localización gestiona la localidad y geografia de la startup Control guarda y getstiona la informacion de la startup

Marta: {{ 
    "sql": "
            --Dimensión de Startups
            CREATE OR REPLACE VIEW DATA_MART.STARTUPS_DIM AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY RECORD_ID) AS startup_surrogate_key, -- Clave subrogada
                RECORD_ID AS startup_natural_key,  -- Clave natural (original)
                NAME AS startup_name,
                SECTOR AS startup_sector,
                SECTOR_DASH AS startup_sector_dash
            FROM
                GOLDEN_ZONE.STARTUPS;

            --Dimensión de Ciudades
            CREATE OR REPLACE VIEW DATA_MART.CIUDADES_DIM AS
            SELECT
                ROW_NUMBER() OVER (ORDER BY RECORD_ID) AS ciudad_surrogate_key, -- Clave subrogada
                RECORD_ID AS ciudad_natural_key,  -- Clave natural (original)
                NOMBRE_CIUDAD AS ciudad_nombre,
                PAIS AS ciudad_pais
            FROM
                GOLDEN_ZONE.CIUDADES;

            --Tabla de Hechos de Inversiones en Startups
            CREATE OR REPLACE VIEW DATA_MART.INVERSIONES_FACT AS
            SELECT
                s.RECORD_ID AS startups_natural_key,  -- Clave natural para enlazar con la dimensión STARTUPS_DIM
                st.startup_surrogate_key,             -- Clave subrogada de la dimensión STARTUPS_DIM
                c.ciudad_surrogate_key,               -- Clave subrogada de la dimensión CIUDADES_DIM
                s.TOTAL_INVERSION_CAPTADA,
                s.CAPTADO_2018,
                s.CAPTADO_2019,
                s.CAPTADO_2020,
                s.CAPTADO_2021,
                s.CAPTADO_2022,
                s.EXITS,
                s.RONDAS
            FROM
                GOLDEN_ZONE.STARTUPS s
            INNER JOIN
                DATA_MART.STARTUPS_DIM st ON s.RECORD_ID = st.startup_natural_key  -- Enlazar con la dimensión de Startups
            INNER JOIN
                DATA_MART.CIUDADES_DIM c ON s.CIUDAD = c.ciudad_natural_key;       -- Enlazar con la dimensión de Ciudades
    
    ",
    "explanation": "Se ha creado un modelado de datos basado en la informacion proporcionada, si necesitas mas informacion sobre alguna parte del modelado, no dudes en preguntar",
    "schema": 
            [
                {{
                "name": "Data_Mart.INVERSIONES_STARTUPS",
                "type": "fact",
                "cols": [
                    {{
                        "name": "RECORD_ID",
                        "key": "pk"
                    }},
                    {{
                        "name": "STARTUP_ID"
                        "key": "fk"
                    }},
                    {{
                        "name": "TOTAL_INVERSION_CAPTADA"
                        "key": ""
                    }},...
                ]
                }},
                {{
                    "name": "STARTUPS_DIM",
                    "type": "dim",
                    "cols":[
                        {{
                            "name": "STARTUP_ID",
                            "key": "pk"
                        }},
                        {{
                            "name": "NOMBRE_STARTUP",
                            "key": ""
                        }},...
                    ]
                }},
                {{
                    "name": "CIUDADES_DIM",
                    "type": "dim",
                    "data": [
                        {{
                            "name": "CIUDAD_ID",
                            "key": "pk"
                        }},
                        {{
                            "name": "NOMBRE_CIUDAD",
                            "key": ""
                        }},...
                    ]
                    "dim": null
                }}
            ]

}}

Usuario: Explicame mas sobre la tabla de hechos
Marta: {{
    "sql": Vuelves a mandar las mismas sentencias que antes,
    "explanation": "La tabla de hechos es la tabla principal de nuestro modelo, en ella se encuentran los datos mas importantes de nuestra empresa. En este caso, la tabla de hechos se llama Data Mart Inversiones_Startups y contiene informacion sobre las inversiones realizadas en las startups, los exits, las rondas de financiacion y las ciudades donde se encuentran las startups.",
    "schema": Vuelves a mandar el mismo esquema que antes
}}



Formato de la informacion de la base de datos:
(column_name, data_type, is_nullable, ordinal_position)

IMPORTANTE TIENES QUE CEÑIRTE ABSOLUTAMENTE A ESTOS DATOS, NO PUEDES AÑADIR MAS INFORMACION NI CAMBIARLA, SOLO PUEDES USAR ESTA INFORMACION PARA CREAR EL MODELO DE DATOS.

"""

ins_kpi = """
    Eres Marta, una experta en consultas SQL y extracción de KPIs, cuyo único objetivo es ayudar a un cliente a generar consultas y buscar KPIs basandote en una base datos que recibirás en el siguiente mensaje. Cada vez que realices un cambio en el esquema, debes devolver ÚNICAMENTE un JSON con 2 componentes y nada de texto adicional fuera del JSON y devuelve los resultados en el mismo idioma en el que te hable el cliente. Los componentes son los siguientes:
    
    "sql": La sentencia SQL completa necesaria para recuperar los datos solicitados por el usuario.
    "explanation": Una explicación de lo que has hecho y por qué, ajustando la extensión de la explicación según la complejidad del cambio.
"""

def get_schema_by_catalog(catalog):
    cursor = conn.cursor()
    try:
        query = f"""
            SELECT SCHEMA_NAME
            FROM {catalog}.INFORMATION_SCHEMA.SCHEMATA;
        """
    
        cursor.execute(query)
        result = cursor.fetchall()
        return result
    
    finally:
        cursor.close()

def get_tables_by_catalog_schema(catalog, schema):
    cursor = conn.cursor()
    try:
        query = f"""
            SELECT TABLE_NAME
            FROM {catalog}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}';
        """
    
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result]
    
    finally:
        cursor.close()

def get_columns_by_table(catalog, schema, table):
    cursor = conn.cursor()
    try:
        query = f"""
            SELECT COLUMN_NAME
            FROM {catalog}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}';
        """
   
        cursor.execute(query)
        result = cursor.fetchall()
        return [row[0] for row in result]
   
    finally:
        cursor.close()

def response_generator(response):
    for word in response.split():
        yield word + " "
        time.sleep(0.05)

client = AzureOpenAI(
  api_key="b954431ec1bf4a9abad9a4aa4f1941bf",
  api_version="2024-08-01-preview",
  azure_endpoint="https://datamartaopenai.openai.azure.com/openai/deployments/gpt-4/chat/completions?api-version=2024-08-01-preview",
)

class ChatBot:
    def __init__(self, system_instruction):
        self.messages = [{"role": "system", "content": system_instruction}]

    def send_message(self, user_message):
        self.messages.append({"role": "user", "content": user_message})
        response = client.chat.completions.create(
            model="gpt-4",
            messages=self.messages,
            timeout=30
        )

        assistant_message = response.choices[0].message.content
        self.messages.append({"role": "assistant", "content": assistant_message})
        print(assistant_message)
        return assistant_message

bot_domain = ChatBot(system_instruction=ins_domain)
bot_datamarta = ChatBot(system_instruction=ins_datamarta)
bot_kpi = ChatBot(system_instruction=ins_kpi)

def home():
    st.markdown("<h1 class='title'>Bienvenido a Lakehouse GenAI</h1>", unsafe_allow_html=True)
    st.markdown("<p class='subtitle'>Una plataforma avanzada de análisis y visualización de datos potenciada por inteligencia artificial</p>", unsafe_allow_html=True)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("<div class='section'>", unsafe_allow_html=True)
        st.markdown("<div class='section-icon'>📊</div>", unsafe_allow_html=True)  # Icono visual
        st.markdown("<h3>Visualización Avanzada</h3>", unsafe_allow_html=True)
        st.markdown("<p>Explora tus datos mediante gráficos interactivos que facilitan la toma de decisiones estratégicas.</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col2:
        st.markdown("<div class='section'>", unsafe_allow_html=True)
        st.markdown("<div class='section-icon'>🤖</div>", unsafe_allow_html=True)  # Icono visual
        st.markdown("<h3>Inteligencia Artificial</h3>", unsafe_allow_html=True)
        st.markdown("<p>Utiliza modelos de IA para obtener insights profundos y accionables a partir de tus datos.</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

    with col3:
        st.markdown("<div class='section'>", unsafe_allow_html=True)
        st.markdown("<div class='section-icon'>⚙️</div>", unsafe_allow_html=True)  # Icono visual
        st.markdown("<h3>Automatización de Procesos</h3>", unsafe_allow_html=True)
        st.markdown("<p>Automatiza flujos de trabajo para simplificar tareas repetitivas y aumentar la eficiencia.</p>", unsafe_allow_html=True)
        st.markdown("</div>", unsafe_allow_html=True)

def domain():
    st.subheader("Selecciona DATABASE y SCHEMA")
    
    if 'control_domain' not in st.session_state:   
        st.session_state.control_domain = True

    if 'domains' not in st.session_state:
        st.session_state.domains = []

    if 'messages_dom' not in st.session_state:
        st.session_state.messages_dom = []

    info_tables = ""
    response = ""
    col1, col2 = st.columns(2)
    with col1:
        # COGER DINAMICAMENTE Y SACAR A OTRA FUNCIÓN
        database = st.selectbox("DATABASE", ["", "STARTUPS", "DATAMARTA", "DATAQUALITY", "FINOPS"])
    with col2:
        if database != '':
            schemas = [""]
            schemas.extend([row[0] for row in get_schema_by_catalog(database)])
            schema = st.selectbox("SCHEMA:", schemas)
        else:
            schema = ""
            
    if schema != '':
        st.write("Pulsa para iniciar la conversación")
        if st.button("Continuar"):
            info_tables = get_tables_by_catalog_schema(database, schema)
            if st.session_state.control_domain:
                st.session_state.control_domain = False
                r = json.loads(bot_domain.send_message("Estas son las tablas que tengo. Identifica los dominios y qué tablas van en cada dominio: \n"+str(info_tables)).replace('\n', "").strip())
                response = r["explanation"]
                st.session_state.domains = r["domains"]
                st.session_state.messages_dom.append({"role": "assistant", "content": response})

    st.markdown("---")

    if st.session_state.messages_dom:
        for message in st.session_state.messages_dom:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    if prompt := st.chat_input("Escribe tu mensaje..."):
        st.session_state.messages_dom.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        response = bot_domain.send_message("Estos son los dominios actuales:\n" + str(st.session_state.domains) + "\n\nY este es el nuevo prompt del usuario:\n" + prompt)
        
        response = json.loads(response.replace('\n', "").strip())
        if "explanation" in response and "domains" in response:
            res = response["explanation"]
            st.session_state.domains = response["domains"]
        else:
            res = response
        
        with st.chat_message("assistant"):
            response_stream = st.write_stream(response_generator(str(res)))
        st.session_state.messages_dom.append({"role": "assistant", "content": str(res)})

    if st.session_state.messages_dom and st.session_state.messages_dom[-1]['role'] == 'assistant':
        st.subheader("Dominio de Datos Propuestos")
        cols = st.columns(2)
        for i, domain in enumerate(st.session_state.domains):
            with cols[i % 2]:
                st.markdown(f"### {domain['name']}")
                st.write("Tablas:")
                for table in domain['tables']:
                    st.markdown(f"- {table}")

def datamarta():
    st.subheader("Selecciona DATABASE y SCHEMA")

    if 'control_marta' not in st.session_state:   
        st.session_state.control_marta = True

    if 'columns_info' not in st.session_state:
        st.session_state.columns_info = {}

    if 'datamart' not in st.session_state:
        st.session_state.datamart = []
    
    if 'messages_marta' not in st.session_state:
        st.session_state.messages_marta = []

    response = ""
    m_c1, m_c2 = st.columns(2)
    with m_c1:
        # COGER DINAMICAMENTE Y SACAR A OTRA FUNCIÓN
        m_database = st.selectbox("DATABASE", ["", "STARTUPS", "DATAMARTA", "DATAQUALITY", "FINOPS"])
    with m_c2:
        if m_database != '':
            schemas = [""]
            schemas.extend([row[0] for row in get_schema_by_catalog(m_database)])
            m_schema = st.selectbox("SCHEMA:", schemas)
            tables = get_tables_by_catalog_schema(m_database, m_schema)
            for table in tables:
                st.session_state.columns_info[table] = get_columns_by_table(m_database, m_schema, table)
        else:
            m_schema = ""

    if m_schema != '':
        st.write("Pulsa para iniciar la conversación")
        if st.button("Continuar"):
            if st.session_state.control_marta:
                st.session_state.control_marta = False
                r = json.loads(bot_datamarta.send_message("Estas son las tablas que tengo y sus campos. \n"+str(st.session_state.columns_info) + 
                                                  "\n\nRecomiendame un DataMart con esos datos y trabaja mano a mano con el usuario. ").replace('\n', "").strip())
                response = r["explanation"]
                st.session_state.datamart = r["datamart"]
                st.session_state.messages_marta.append({"role": "assistant", "content": response})
    
    st.markdown("---")
 
    if st.session_state.messages_marta:
        for message in st.session_state.messages_marta:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    if prompt := st.chat_input("Escribe tu mensaje..."):
        st.session_state.messages_marta.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        response = bot_datamarta.send_message("Estos es el data mart actual:\n" + str(st.session_state.datamart) + "\n\nY este es el nuevo prompt del usuario:\n" + prompt)
        
        response = json.loads(response.replace('\n', "").strip())
        if "explanation" in response and "datamart" in response:
            res = response["explanation"]
            st.session_state.datamart = response["datamart"]
        else:
            res = response
        
        with st.chat_message("assistant"):
            response_stream = st.write_stream(response_generator(str(res)))
        st.session_state.messages_marta.append({"role": "assistant", "content": str(res)})

    if st.session_state.datamart:
        st.subheader("Schemas Generados")
        cols = st.columns(2)
        for i, esquema in enumerate(st.session_state.datamart):
            with cols[i % 2]:
                st.markdown(f"### {esquema['name']} ({esquema['type']})")
                st.write("Columnas:")
                for col in esquema['cols']:
                    st.markdown(f"- **{col['name']}** ({col['key']})")

def kpi_assistant():
    st.subheader("Selecciona DATABASE y SCHEMA")
    if 'messages_kpi' not in st.session_state:
        st.session_state.messages_kpi = []

    if 'columns_info' not in st.session_state:
        st.session_state.columns_info = {}

    response = ""
    tables = []
    k_c1, k_c2 = st.columns(2)
    with k_c1:
        database = st.selectbox("DATABASE", ["", "STARTUPS", "DATAMARTA", "DATAQUALITY", "FINOPS"])
    with k_c2:
        if database != '':
            schema = st.selectbox("SCHEMA:", [row[0] for row in get_schema_by_catalog(database)])

            tables = get_tables_by_catalog_schema(database, schema)
            for table in tables:
                st.session_state.columns_info[table] = get_columns_by_table(database, schema, table)
            
            bot_kpi.send_message("Estas son las tablas que tengo y sus campos. \n" + str(st.session_state.columns_info) + 
                                                  "\n\nUtiliza dicha información para ayudar al cliente con sus demandas.")

    st.markdown("---")

    if st.session_state.messages_kpi:
        for message in st.session_state.messages_kpi:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])

    if prompt := st.chat_input("Escribe tu pregunta sobre KPIs..."):
        st.session_state.messages_kpi.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)

        prompt_to_bot = f"Pregunta del usuario: {prompt}"

        prompt_to_bot += (f"\nPor favor, genera la consulta SQL en formato Snowflake, "
                                f"teniendo en cuenta que DATABASE_NAME es el nombre de la base de datos seleccionada ({database}) "
                                f"y SCHEMA_NAME es el nombre del esquema seleccionado ({schema}). "
                                "Asegúrate de incluir el nombre de la base de datos y el esquema en la cláusula FROM. "
                                "Por ejemplo:\n\n"
                                f"SELECT * \n"
                                f"FROM {database}.{schema}.ACQUISITIONS....")

        response = bot_kpi.send_message(prompt_to_bot)
        response = json.loads(response.replace('\n', "").strip())

        if "sql" in response:
            sql_query = response["sql"]
        else:
            sql_query = "No se pudo generar una consulta SQL."

        with st.chat_message("assistant"):
            st.markdown(f"**Consulta SQL generada:**\n```sql\n{sql_query}\n```")

        st.session_state.messages_kpi.append({"role": "assistant", "content": f"Consulta SQL generada: {sql_query}"})
 
        try:
            result_df = execute_sql_query(sql_query)
            st.markdown("### Resultado de la consulta:")
            st.dataframe(result_df)
 
        except Exception as e:
            st.error(f"Error al ejecutar la consulta SQL: {e}")

def execute_sql_query(sql_query):
    cursor = conn.cursor()
    try:
        cursor.execute(sql_query)
        result_df = cursor.fetch_pandas_all()
        return result_df
    finally:
        cursor.close()

def main():
    st.markdown("<h1 style='text-align: center;'>Lakehouse GenAI</h1>", unsafe_allow_html=True)

    page = option_menu(
        menu_title=None,
        options=["Home", "DomAIn", "Data Marta", "KPIs Assistant"],
        default_index=0,
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#232d4b"},
            "icon": {"color": "white", "font-size": "15px"},
            "nav-link": {"font-size": "15px", "text-align": "center", "margin": "0px", "color": "white", "--hover-color": "#006fbb"},
            "nav-link-selected": {"background-color": "#006fbb"},
        }
    )

    if page == "Home":
        home()
    elif page == "DomAIn":
        domain()
    elif page == "Data Marta":
        datamarta()
    elif page == "KPIs Assistant":
        kpi_assistant()

if __name__ == "__main__":
    main()

st.markdown("""
    <style>
    /* Fondo degradado para la home */
    .main {
        background: linear-gradient(135deg, #f0f4f8, #e0f7f3);  /* Degradado claro */
        padding: 2rem;
    }
    .title {
        font-size: 2.5rem;
        font-weight: bold;
        color: #232d4b;  /* Strong Blue */
        text-align: center;
    }
    .subtitle {
        font-size: 1.2rem;
        color: #005573;  /* Neutral Blue */
        text-align: center;
        margin-bottom: 2rem;
    }
    .section {
        text-align: center;
        margin-bottom: 2rem;
    }
    .section h3 {
        font-weight: bold;
        color: #00aa9b;  /* Mineral Green */
    }
    .section p {
        color: #232d4b;  /* Strong Blue */
        margin-top: -10px;
    }
    .button-section {
        text-align: center;
        margin-top: 2rem;
    }
    .stButton>button {
        background-color: #f04641;  /* Accent Red */
        color: white;
        border: none;
        padding: 1rem 2rem;
        font-weight: bold;
        border-radius: 5px;
    }
    .stButton>button:hover {
        background-color: #d83c3a;
    }
    .section-icon {
        font-size: 3rem;
        color: #00aa9b;  /* Mineral Green */
        margin-bottom: 1rem;
    }
    .chat-container {
        height: 400px;
        overflow-y: scroll;
        border: 1px solid #ccc;
        padding: 10px;
        background-color: #005573;  /* Neutral Blue */
        color: white;
        border-radius: 10px;
    }
    .chat-input {
        position: sticky;
        bottom: 0;
        width: 100%;
        background-color: #232d4b;  /* Strong Blue */
        padding: 10px;
        border-radius: 5px;
        color: white;
    }
    .domain-box {
        padding: 15px;
        background-color: #00aa9b;  /* Mineral Green */
        border-radius: 10px;
        color: white;
        margin-bottom: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1); /* Sombra para dar efecto visual */
        font-size: 16px;
    }
    .domain-title {
        font-weight: bold;
        color: #f7f9fc; /* Color claro para destacar el título */
    }
    .domain-table {
        font-style: italic;
        margin-bottom: 5px;
    }
    .action-button {
        background-color: #f04641;  /* Accent Red */
        color: white;
        border-radius: 5px;
        padding: 10px 20px;
        border: none;
        font-weight: bold;
        text-align: center;
    }
    .action-button:hover {
        background-color: #d83c3a;
    }
    /* Estilos para los selectbox (Database y Schema) */
    .stSelectbox label {
        color: #00aa9b;  /* Mineral Green */
        font-weight: bold;
    }
    .stSelectbox div[data-baseweb="select"] {
        background-color: #232d4b;  /* Strong Blue */
        color: white;
        border-radius: 5px;
        border: 2px solid #00aa9b;  /* Mineral Green */
    }
    .stSelectbox div[data-baseweb="select"]:hover {
        border: 2px solid #f04641;  /* Accent Red para hover */
    }
    .stSelectbox div[data-baseweb="select"] .stSelectbox__single-value {
        color: white;  /* Texto blanco en el selectbox */
    }
    </style>
""", unsafe_allow_html=True)

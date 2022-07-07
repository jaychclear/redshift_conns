import redshift_connector

def get_conn(): 
    
    conn = redshift_connector.connect(
        iam=True,
        ssl=True,
        host='gallium-global-prd-redshift-monetise.c67tqkser81t.eu-west-1.redshift.amazonaws.com',
        port=5439,
        database='prd',
        db_user='',
        cluster_identifier='gallium-global-prd-redshift-monetise',
        region='eu-west-1',
        login_url='https://clearscore.okta-emea.com/home/clearscore_awsanalyticsgalliumglobalprdredshiftmonetise_1/0oa6h62xwaca5R7440i7/aln6h65hizqZZ8LYK0i7',
        credentials_provider='BrowserSamlCredentialsProvider',
        user='',
        password=''
    )
    conn.rollback()
    conn.autocommit = True
    return conn
    
def sql_2_execute(sql_stat, conn):
    c = conn.cursor()
    c.execute(sql_stat)
    return None

def sql_2_df(sql_stat, conn):
    c = conn.cursor()
    c.execute(sql_stat)
    result= c.fetch_dataframe()
    return result
    
def create_sql_list(sql_stat):
    temp_sql_list = [s.strip() for s in sql_stat.split(';')]
    temp_sql_list = list(filter(None, temp_sql_list))
    return temp_sql_list
    
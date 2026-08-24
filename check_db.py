import psycopg2
conn = psycopg2.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
print('tables:', cur.fetchall())
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'request_queue'")
print('columns:', cur.fetchall())
conn.close()

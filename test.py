import psycopg2
import requests
import re


# Подключение к json файлу
response = requests.get('https://raw.githubusercontent.com/Hipo/university-domains-list/master/world_universities_and_domains.json')
data = response.json()


# Подключение к базе данных
conn = psycopg2.connect(
    database="postgres", user='postgres', password='postgres', host='localhost', port='5432'
)
cursor = conn.cursor()

# Функция определения типа заведения
def search_type(name):
    if re.search(r'\b(College|Colleges|Collège|Colegio)\b', name, flags=re.IGNORECASE):
        return 'College'
    elif re.search(r'\b(Institute|Institut|Institutions|Instituts|Instituto|Institución|Institucion)\b', name, flags=re.IGNORECASE):
        return 'Institute'
    elif re.search(r'\b(Universiteit|Universitat|Univesidade|Univerzitet|Universiti|Univerisity|Université|Universität|Universitas|Università|Universita|Universidade|Universidad|Üniversitesi|University|Uinversity|Universitatea)\b', name, flags=re.IGNORECASE):
        return 'University'
    else: return None


# Обработка объектов
new_institutions = []
for item in data:
    name = item.get('name')
    cursor.execute("SELECT * FROM institutions WHERE name = %s", (name,))
    existing_university = cursor.fetchone()
    if existing_university:
        continue  
    country = item.get('country')
    alpha_two_code = item.get('alpha_two_code')
    state_province = item.get('state-province')
    type_institution = search_type(name)
    cursor.execute("INSERT INTO institutions (name, country, alpha_two_code, state_province, type_institution) VALUES (%s, %s, %s, %s, %s)", (name, country, alpha_two_code, state_province, type_institution))
    conn.commit()
    new_institutions.append((name, country, alpha_two_code, state_province, type_institution))

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'College' ORDER BY name")
result = cursor.fetchall()

print("Список всех колледжей:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'Institute' ORDER BY name")
result = cursor.fetchall()

print("Список всех институтов:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution = 'University' ORDER BY name")
result = cursor.fetchall()

print("Список всех университетов:")
for row in result:
    print(row)

cursor.execute("SELECT * FROM institutions WHERE type_institution IS NULL ORDER BY name")
result = cursor.fetchall()

print("Список учреждений, у которых тип не был определен:")
for row in result:
    print(row)

cursor.close()
conn.close()

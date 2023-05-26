import psycopg2
import pandas as pd
import csv
conn = psycopg2.connect(
host="localhost", port=1122, database="chicago_crimes", user="project", password="12345"
)
cur = conn.cursor()

sql1='''CREATE TABLE public.chicago (
	id int NULL,
	case_number varchar NULL,
	date varchar NULL,
	block varchar NULL,
	iucr varchar NULL,
	primary_type varchar NULL,
	description varchar NULL,
	location_description varchar NULL,
	arrest boolean NULL,
	domestic boolean NULL,
	beat int NULL,
	district int NULL,
	ward int NULL,
	community_area int NULL,
	fbi_code varchar NULL,
	x_coordinate float4 NULL,
	y_coordinate float4 NULL,
	year varchar NULL,
	update_on varchar NULL,
	latitude float4 NULL,
	longitude float4 NULL,
	location varchar NULL
);'''

cur.execute(sql1)

sql2='''CREATE TABLE public.la (
	dr_no int NULL,
	date_rptd varchar NULL,
	date_occ varchar NULL,
	time_occ varchar NULL,
	area int NULL,
	area_name varchar NULL,
	rpt_dist_no int NULL,
	part_1_2 int NULL,
	crm_cd int NULL,
	crm_cd_desc varchar NULL,
	mocodes varchar NULL,
	vict_age int NULL,
	vict_sex varchar NULL,
	vict_descent varchar NULL,
	premis_cd int NULL,
	premis_desc varchar NULL,
	weapon_used_cd int NULL,
	weapon_desc varchar NULL,
	status varchar NULL,
	status_desc varchar NULL,
	crm_cd_1 int NULL,
   crm_cd_2 int NULL,
   crm_cd_3 int NULL,
   crm_cd_4 int NULL,
	location varchar NULL,
	cross_street varchar NULL,
	lat float4 NULL,
	lon float4 NULL
);'''

cur.execute(sql2)

data = pd.read_csv("chicago_crimes.csv", sep=",",chunksize=100000, low_memory=False)
sql_string = "INSERT INTO public.chicago (id, case_number, date, block, iucr, primary_type, description, location_description, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, year, update_on, latitude, longitude, location) VALUES"
#coloums = list(data.columns)
#columns_string = "(" + ",".join(coloums) + ")"
#sql_string = sql_string + columns_string + " VALUES "




print(sql_string)


for chunk in data:
   values_list = []
   sql_string = "INSERT INTO public.chicago (id, case_number, date, block, iucr, primary_type, description, location_description, arrest, domestic, beat, district, ward, community_area, fbi_code, x_coordinate, y_coordinate, year, update_on, latitude, longitude, location) VALUES"


   for i in range(len(chunk)):
       row = chunk.iloc[i]
       tmp_string = "({},'{}','{}','{}','{}','{}','{}','{}',{},{},{},{},{},{},'{}',{},{},{},'{}',{},{},'{}')".format(row["ID"], row["Case Number"], row["Date"], row["Block"], row["IUCR"], row["Primary Type"], row["Description"], row["Location Description"], row["Arrest"], row["Domestic"], row["Beat"], row["District"], row["Ward"], row["Community Area"], row["FBI Code"], row["X Coordinate"], row["Y Coordinate"], row["Year"], row["Updated On"], row["Latitude"], row["Longitude"], row["Location"])
       values_list.append(tmp_string)


   values_string = ",".join(values_list)
   sql_string += values_string
   sql_string = sql_string.replace("nan","null")


cur.execute(sql_string)


data = pd.read_csv("la_crimes.csv", sep=",",chunksize=100000, low_memory=False)
sql_string = "INSERT INTO public.la (dr_no, date_rptd, date_occ, time_occ, area, area_name, rpt_dist_no, part_1_2, crm_cd, crm_cd_desc, mocodes, vict_age, vict_sex, vict_descent, premis_cd, premis_desc, weapon_used_cd, weapon_desc, status, status_desc, crm_cd_1, crm_cd_2, crm_cd_3, crm_cd_4, location, cross_street, lat, lon) VALUES"
#coloums = list(data.columns)
#columns_string = "(" + ",".join(coloums) + ")"
#sql_string = sql_string + columns_string + " VALUES "


print(sql_string)



for chunk in data:
   values_list = []
   sql_string = "INSERT INTO public.la (dr_no, date_rptd, date_occ, time_occ, area, area_name, rpt_dist_no, part_1_2, crm_cd, crm_cd_desc, mocodes, vict_age, vict_sex, vict_descent, premis_cd, premis_desc, weapon_used_cd, weapon_desc, status, status_desc, crm_cd_1, crm_cd_2, crm_cd_3, crm_cd_4, location, cross_street, lat, lon) VALUES"


   for i in range(len(chunk)):
       row = chunk.iloc[i]
       tmp_string = "({},'{}','{}','{}',{},'{}',{},{},{},'{}','{}',{},'{}','{}',{},'{}',{},'{}','{}','{}',{},{},{},{},'{}','{}',{},{})".format(row["DR_NO"], row["Date Rptd"], row["DATE OCC"], row["TIME OCC"], row["AREA "], row["AREA NAME"], row["Rpt Dist No"], row["Part 1-2"], row["Crm Cd"], row["Crm Cd Desc"], row["Mocodes"], row["Vict Age"], row["Vict Sex"], row["Vict Descent"], row["Premis Cd"], row["Premis Desc"], row["Weapon Used Cd"], row["Weapon Desc"], row["Status"], row["Status Desc"], row["Crm Cd 1"], row["Crm Cd 2"], row["Crm Cd 3"], row["Crm Cd 4"], row["LOCATION"], row["Cross Street"], row["LAT"], row["LON"])
       values_list.append(tmp_string)


   values_string = ",".join(values_list)
   sql_string += values_string
   sql_string = sql_string.replace("nan","null")

sql3 = '''
Create table public.eight as
select primary_type, count(primary_type)
from public.chicago
where year = '2008'
group by primary_type '''

cur.execute(sql3)

sql4 = '''ALTER TABLE public.eight 
ADD id int unique primary key not null generated by default as identity'''

cur.execute(sql4)

sql5 = '''
Create table public.twelve as
select primary_type, count(primary_type)
from public.chicago
where year = '2012'
group by primary_type '''

cur.execute(sql5)

sql6 = '''ALTER TABLE public.twelve
ADD id int unique primary key not null generated by default as identity'''

cur.execute(sql6)

sql7 = '''Create table public.crimess as
select eight.primary_type, eight.count as crises, twelve as notcrises
from eight join twelve on twelve.id = eight.id
'''
cur.execute(sql7)

sql8 = ''' Create table public.childabusehome as
select primary_type, count(case when year = '2013' then 1 else null end) as thirteen,
count(case when year = '2014' then 1 else null end) as fourteen,
count(case when year = '2015' then 1 else null end) as fifteen 
from public.chicago
where domestic = true
and primary_type = 'OFFENSE INVOLVING CHILDREN'
group by primary_type
'''
cur.execute(sql8)

sql9 = ''' Create table public.childabusenhome as
select primary_type, count(case when year = '2013' then 1 else null end) as thirteen,
count(case when year = '2014' then 1 else null end) as fourteen,
count(case when year = '2015' then 1 else null end) as fifteen 
from public.chicago
where domestic = false
and primary_type = 'OFFENSE INVOLVING CHILDREN'
group by primary_type'''

cur.execute(sql9)

sql10 = '''Create table public.female_young as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'F'
and crm_cd_desc = 'ROBBERY'
and vict_age between '18' and '35'
group by crm_cd_desc
'''
cur.execute(sql10)

sql11 = '''Alter table public.female_young
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql11)

sql12 = '''Create table public.female_adult as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'F'
and crm_cd_desc = 'ROBBERY'
and vict_age between '36' and '55'
group by crm_cd_desc
'''
cur.execute(sql12)

sql12 = '''Alter table public.female_adult
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql12)

sql13 = '''Create table public.female_elder as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'F'
and crm_cd_desc = 'ROBBERY'
and vict_age between '56' and '70'
group by crm_cd_desc
'''
cur.execute(sql13)

sql14 = '''Alter table public.female_elder
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql14)

sql15 = '''Create table public.male_young as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'M'
and crm_cd_desc = 'ROBBERY'
and vict_age between '18' and '35'
group by crm_cd_desc
'''
cur.execute(sql15)

sql16 = '''Alter table public.male_young
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql16)

sql17 = '''Create table public.male_adult as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'M'
and crm_cd_desc = 'ROBBERY'
and vict_age between '36' and '55'
group by crm_cd_desc
'''
cur.execute(sql17)

sql18 = '''Alter table public.male_adult
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql18)

sql19 = '''Create table public.male_elder as 
select crm_cd_desc, count(crm_cd_desc)
from public.la
where vict_sex = 'M'
and crm_cd_desc = 'ROBBERY'
and vict_age between '56' and '70'
group by crm_cd_desc
'''
cur.execute(sql19)

sql20 = '''Alter table public.male_elder
add id int unique primary key not null generated by default as identity
'''

cur.execute(sql20)

sql21 = '''Create table robbery_young as
select female_young.crm_cd_desc, female_young.count as female, male_young.count as male
from female_young join male_young on male_young.id = female_young.id
'''

cur.execute(sql21)

sql22 = '''Create table robbery_adult as
select female_adult.crm_cd_desc, female_adult.count as female, male_adult.count as male
from female_adult join male_adult on male_adult.id = female_adult.id
'''

cur.execute(sql22)

sql23 = '''Create table robbery_elder as
select female_elder.crm_cd_desc, female_elder.count as female, male_elder.count as male
from female_elder join male_elder on male_elder.id = female_elder.id
'''
cur.execute(sql23)

cursor = conn.cursor()
cursor.execute(sql_string)
conn.commit()



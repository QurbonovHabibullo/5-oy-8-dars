import psycopg2

class DataBase:
    def __init__(self) -> None:
        self.database = psycopg2.connect(
            database = '8-dars vazifa',
            user = 'postgres',
            host = 'localhost',
            password = '1'
        )
        
    def manager(self,sql,*args,commit=False,fetchone=False,fetchall=False):
        with self.database as db:
            with db.cursor() as cursor:
                cursor.execute(sql,args)
                if commit:
                    db.commit()
                elif fetchone:
                    return cursor.fetchone()
                elif fetchall:
                    return cursor.fetchall()
                
    def drop_tables(self):           
        sqls =[
            'drop table if exists employees cascade;',
            'drop table if exists departments cascade;',
            'drop table if exists projects cascade;'
            ]

        for sql in sqls:
            self.manager(sql,commit=True)
 
                
    def create_tables(self):
        sqls = [
        '''
        create table if not exists  employees(
            id serial primary key,
            first_name varchar(50) not null,
            last_name varchar(50) not null,
            position text not null,
            salary integer,
            hire_date date,
            department_id integer     
         );''',
                
        '''
        create table if not exists departments(
            id serial primary key,
            department_name varchar(50) not null
        );''',
        
        '''
        create table if not exists projects(
            id serial primary key,
            project_name varchar(50) not null,
            start_date date,
            end_date date,
            budget integer
        );'''
        ]
        for sql in sqls:
            self.manager(sql,commit=True)
#----------------------------------------------------------   
    def insert_into_employees(self,first_name,last_name,position,salary,hire_date,department_id):
        sql = '''insert into employees(first_name,last_name,position,salary,hire_date,department_id) values (%s,%s,%s,%s,%s,%s)'''
        self.manager(sql,first_name,last_name,position,salary,hire_date,department_id,commit=True)
    
    def select_employees(self):
        sql = 'select * from employees'
        return self.manager(sql,fetchall=True)
# ----------------------------------------------------------
    def insert_into_departments(self,department_name):
        sql = '''insert into departments(department_name) values (%s)'''
        self.manager(sql,department_name,commit=True)
        
    def select_departments(self):
        sql = 'select * from departments'
        return self.manager(sql,fetchall=True)
# ---------------------------------------------------------
    def insert_into_projects(self,project_name,start_date,end_date,budget):
        sql = '''insert into projects(project_name,start_date,end_date,budget) values (%s,%s,%s,%s)'''
        self.manager(sql,project_name,start_date,end_date,budget,commit=True)
        
    def select_projects(self):
        sql = 'select * from projects'
        return self.manager(sql,fetchall=True)
    
    def ustun_birlashtr(self):
        sql = '''select first_name ||' '|| last_name as full_name from employees;'''
        return self.manager(sql,fetchall=True)

    def order_by(self,salary):
        sql = f'''select {salary} from employees order by salary desc;'''
        return self.manager(sql,fetchall=True)
    
    def where(self,salary):
        sql = f'''select {salary} from employees where {salary}> {2500}; '''
        return self.manager(sql,fetchall=True)
    
    def limit(self):
        sql = '''select * from employees order by salary desc limit 3;'''
        return self.manager(sql,fetchall=True)
        
    def fetch(self):
        sql = '''select * from employees order by salary desc fetch first 3 row only;'''
        return self.manager(sql,fetchall=True)
        
    def where_in(self):
        sql = '''select * from employees where salary in(2400,3000);''' 
        return self.manager(sql,fetchall=True)
    
    def between(self):
        sql = '''select * from employees where salary between 2000 and 3000;'''
        return self.manager(sql,fetchall=True)

    def like(self):
        sql = '''select first_name from employees where first_name like %s;'''
        return self.manager(sql,'%a%',fetchall=True)
        
    def is_null(self):
        sql = '''select * from projects where end_date is null;'''
        return self.manager(sql,fetchall=True)

    def group_by(self):
        sql = '''select department_id, avg(salary) from employees group by department_id; '''
        return self.manager(sql,fetchall=True)
        
db = DataBase()
db.drop_tables()
db.create_tables()
# ---------------------------------------
employees = [
    ('Ali','Karimov','Manager',3000,'2020-03-15',1),
    ('Nodira ','Toirova ','Developer ',2500,'2021-05-10',2),
    ('Shoxruh ','Abdullayev','Designer ',2200 ,'2022-01-22',3),
    ('Zarina ','Abdullayeva','HR Specialist',1800,'2019-11-11',1),
    ('Jasur ','Aliev','Developer',2400,'2023-02-01',2),
    
]
for empl in employees:
    db.insert_into_employees(*empl)
# ------------------------------
departments  = [
   ('Administration'),
   ('IT'),
   ('Design') 
]
for depart in departments:
    db.insert_into_departments(depart)
# ----------------------------------
projects = [
    ('New Website','2023-01-10','2023-06-30',50000),
    ('Mobile App','2022-08-15','2023-03-20',30000),
    ('CRM System','2024-02-01',None,60000)
]
for project in projects:
    db.insert_into_projects(*project)
# -------------------------------------------
print("birlashtirilgan ism familya:",db.ustun_birlashtr())
print(db.select_employees())
print(db.select_departments())
print(db.select_projects())
print(db.order_by('salary'))
print(db.where('salary'))
print(db.limit())
print(db.fetch())
print(db.where_in())
print(db.between())
print(db.like())
print(db.is_null())
print(db.group_by())


Select * from departments;
Select * from dept_emp;
Select * from dept_manager;
Select * from employees;
Select * from salaries;
Select * from titles;
Select * from departments_dup;

# Emp Number, Name, Age from Employees table where First Name is Georgi and Last Name starts with F and Hiring Date is after 1980
Select emp_no,first_name,last_name,2024-(year(birth_date)) as age,gender from employees where first_name='Georgi' and last_name like 'F%' and year(hire_date) > 1980;
# Highest Salary from Salary table
Select * from salaries where salary in (Select max(salary) from salaries);
# Second Highest Salary from Salary table
Select * from salaries order by salary desc limit 1 offset 1;
# Second lowest salary from Salary table
Select * from salaries order by salary asc limit 1 offset 1;
# Count of all employes getting salary of greater than 150000
Select count(*) as cnt from salaries where salary > 150000;
# What is the most offered salary
Select salary,count(*) as cnt from salaries group by 1 order by cnt desc limit 3;
# How many unique job titles exist?
Select count(Distinct title) from titles;
# Year wise average of all salaries
Select year(from_date) as fr_dt,year(to_date) as to_dt,format(avg(salary),0) as avg_yrly_salary from salaries group by 1,2 order by fr_dt asc;
# Year and Month wise average of all salaries
Select year(from_date) as fr_dt,year(to_date) as to_dt,format(avg(salary),0) as avg_yrly_salary,format((avg(salary)/12),0) as avg_mthly_salary from salaries group by 1,2 
order by fr_dt;
# Replace null values from dept_name column in departemtns_dup table with 'It is Null' string
Select dept_no,ifnull(dept_name,'It is Null') as dept_name from departments_dup;
# Return first Non Null value from dept_manager column in departments_dup table using Coalesce
Select dept_no,dept_name,coalesce(dept_manager,dept_name,'Null') as dept_manager from departments_dup order by dept_no asc;
# Enter a dummy column with fake values fromdepartments_dup table
Select *,coalesce('fake') as fake_col from departments_dup;
# Create a View by Joining employees, titles and salaries tables 
Drop view if exists temp_view;
Create view temp_view as
Select t3.emp_no,t3.title,t3.first_name,t3.last_name,t3.gender,t3.hire_date,t4.salary from
(Select t1.emp_no,t2.title,t1.first_name,t1.last_name,t1.gender,t1.hire_date from employees t1 
inner join titles t2 on ((t1.emp_no = t2.emp_no) and (t1.hire_date=t2.from_date))) t3 inner join salaries t4 on 
((t3.emp_no=t4.emp_no) and (t3.hire_date=t4.from_date));
Select * from temp_view;

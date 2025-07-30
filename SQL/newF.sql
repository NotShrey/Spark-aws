use newstart;

-- CREATE TABLE DEPT (
--     DEPTNO INT PRIMARY KEY,
--     DNAME VARCHAR(20),
--     LOC VARCHAR(20)
-- );

-- CREATE TABLE EMP (
--     EMPNO INT PRIMARY KEY,
--     ENAME VARCHAR(20),
--     JOB VARCHAR(20),
--     MGR INT,
--     HIREDATE DATE,
--     SAL DECIMAL(10, 2),
--     COMM DECIMAL(10, 2),
--     DEPTNO INT,
-- FOREIGN KEY (DEPTNO) REFERENCES DEPT(DEPTNO)
-- );

-- INSERT INTO DEPT (DEPTNO, DNAME, LOC) VALUES
-- (10, 'ACCOUNTING', 'NEW YORK'),
-- (20, 'RESEARCH', 'DALLAS'),
-- (30, 'SALES', 'CHICAGO'),
-- (40, 'OPERATIONS', 'BOSTON');

-- INSERT INTO EMP (EMPNO, ENAME, JOB, MGR, HIREDATE, SAL, COMM, DEPTNO) VALUES
-- (7369, 'SMITH', 'CLERK', 7902, '1980-12-17', 800, NULL, 20),
-- (7499, 'ALLEN', 'SALESMAN', 7698, '1981-02-20', 1600, 300, 30),
-- (7521, 'WARD', 'SALESMAN', 7698, '1981-02-22', 1250, 500, 30),
-- (7566, 'JONES', 'MANAGER', 7839, '1981-04-02', 2975, NULL, 20),
-- (7654, 'MARTIN', 'SALESMAN', 7698, '1981-09-28', 1250, 1400, 30),
-- (7698, 'BLAKE', 'MANAGER', 7839, '1981-05-01', 2850, NULL, 30),
-- (7782, 'CLARK', 'MANAGER', 7839, '1981-06-09', 2450, NULL, 10),
-- (7788, 'SCOTT', 'ANALYST', 7566, '1987-04-19', 3000, NULL, 20),
-- (7839, 'KING', 'PRESIDENT', NULL, '1981-11-17', 5000, NULL, 10),
-- (7844, 'TURNER', 'SALESMAN', 7698, '1981-09-08', 1500, 0, 30),
-- (7876, 'ADAMS', 'CLERK', 7788, '1987-05-23', 1100, NULL, 20),
-- (7900, 'JAMES', 'CLERK', 7698, '1981-12-03', 950, NULL, 30),
-- (7902, 'FORD', 'ANALYST', 7566, '1981-12-03', 3000, NULL, 20),
-- (7934, 'MILLER', 'CLERK', 7782, '1982-01-23', 1300, NULL, 10);



-- Questions on Single-Row Subqueries:
-- ***********************************
-- 1.	Find the name of the employee with the highest salary.
select ENAME, SAL
from EMP
where SAL = (select max(SAL) from EMP);

select * from EMP;

-- 2.	List the details of the department where the employee with the minimum salary works.

SELECT D.*
FROM DEPT D
JOIN EMP E ON D.DEPTNO = E.DEPTNO
WHERE E.SAL = (SELECT MIN(SAL) FROM EMP);

-- 3.	Find the name of the employee who works in the department located in ‘NEW YORK’.

select ENAME
from EMP e
join dept d
on e.DEPTNO = d.DEPTNO
where d.LOC = 'NEW YORK';

-- 4.	Find the salary of the employee named ‘SMITH’.

select SAL
from EMP
where ENAME = 'SMITH';

-- 5.	List the department name of the employee with employee number 7839.

SELECT ENAME 
FROM EMP 
WHERE EMPID = (SELECT EMPID FROM EMP WHERE EMPID = 7839);

-- 6.	Find the name and job title of the employee earning the same salary as the highest-paid employee in department 10.

select ENAME,JOB
from EMP
where SAL = (select max(SAL)
from EMP e
join DEPT d on e.DEPTNO = d.DEPTNO
where d.DEPTNO = '10');

select * from DEPT;
select * from EMP;

-- 7.	Find the location of the department where ‘KING’ works.
 
select d.LOC
from EMP e
join DEPT d on e.DEPTNO = d.DEPTNO
where e.ENAME = 'KING';

-- 8.	Find the highest salary in the department where the employee ‘FORD’ works.

select max(SAL)
from EMP
where DEPTNO = 
(select DEPTNO
from EMP
where ENAME = 'FORD');

-- 9.	Retrieve the job title of the employee with the second-highest salary.

SELECT ENAME,JOB
FROM EMP
ORDER BY SAL DESC
LIMIT 1 OFFSET 1;


SELECT JOB
FROM (
    SELECT JOB, DENSE_RANK() OVER (ORDER BY SAL DESC) AS rnk
    FROM EMP
) AS ranked
WHERE rnk = 2
LIMIT 1;

-- 10.	Find the name of the department with the lowest average salary.

select d.DNAME
from EMP e
join DEPT d on e.DEPTNO = d.DEPTNO
where e.SAL = (select min(SAL) from EMP);




-- Questions on Multi-Row Subqueries:
-- ***********************************
select * from EMP;
select * from DEPT;
-- 1.	List the names of employees who work in departments located in ‘NEW YORK’ or ‘DALLAS’.

SELECT e.ENAME
FROM EMP e
JOIN DEPT d ON e.DEPTNO = d.DEPTNO
WHERE e.DEPTNO IN (
    SELECT DEPTNO
    FROM DEPT
    WHERE LOC = 'NEW YORK' OR LOC = 'DALLAS'
);

-- 2.	Retrieve the names of employees who work in the department with more than three employees.

SELECT ENAME
FROM EMP
WHERE DEPTNO IN (
    SELECT DEPTNO
    FROM EMP
    GROUP BY DEPTNO
    HAVING COUNT(*) > 3
);

-- 3.	Find the names of employees who have the same job as ‘KING’.


SELECT ENAME
FROM EMP
WHERE JOB = (
    SELECT JOB
    FROM EMP
    WHERE ENAME = 'KING'
);


-- 4.	List the employees who earn more than any employee in department 10.
select ENAME
from EMP
where SAL > any
(select SAL
from EMP
where DEPTNO = 10);


-- 5.	Retrieve the names of employees who earn less than the minimum salary of any department other than their own.
SELECT E1.ENAME
FROM EMP E1
WHERE E1.SAL < ANY (
    SELECT MIN(E2.SAL)
    FROM EMP E2
    WHERE E2.DEPTNO <> E1.DEPTNO
    GROUP BY E2.DEPTNO
);


-- 6.	Find employees who earn a salary greater than the salary of all employees in department 30.
select ENAME
FROM EMP
where SAL >
(select max(SAL)
from EMP
where DEPTNO = '30');

-- 7.	Retrieve the names of employees who work in departments with at least one employee earning more than 3000.

SELECT ENAME
FROM EMP
WHERE DEPTNO IN (
    SELECT DISTINCT DEPTNO
    FROM EMP
    WHERE SAL > 3000
);


-- SubQueries - Hands-On: Complex Joins with SQ:

CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100)
);

CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

INSERT INTO customers (id, name, email) VALUES
(1, 'Ajay', 'Ajay@example.com'),
(2, 'Babu', 'Babu@example.com'),
(3, 'Chandhru', 'Chandhru@example.com');

INSERT INTO orders (id, customer_id, order_date, amount) VALUES
(1, 1, '2023-06-01', 100.00),
(2, 1, '2023-06-15', 150.00),
(3, 2, '2023-06-10', 200.00),
(4, 3, '2023-06-05', 300.00),
(5, 3, '2023-06-20', 250.00);

select * from customers;
select * from orders;

-- -- Tasks to be performed:

-- Write a query to find the latest order placed by each customer.

SELECT c.name, MAX(o.order_date) AS latest_order
FROM customers c
JOIN orders o ON c.id = o.customer_id
GROUP BY c.name;


-- Write a query to lists all customers and the total amount of their orders.


SELECT c.id, c.name, SUM(o.amount) AS total_amount
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
GROUP BY c.id, c.name;


-- Write a query to find all customers who have placed an order with an amount greater than the average order amount.

SELECT DISTINCT c.name
FROM customers c
JOIN orders o ON c.id = o.customer_id
WHERE o.amount > (
    SELECT AVG(amount)
    FROM orders
);



-- Write a query that finds the total sales amount and the number of orders for each customer who has placed at least one order in the last month.




-- Joins - Hands-On: Complex Joins: With Aggregation Functions and Date Functions:

-- -- Joins in Detail:

-- CREATE TABLE Department (
--     dept_id INT PRIMARY KEY,
--     dept_name VARCHAR(50) NOT NULL
-- );

-- CREATE TABLE Employee (
--     emp_id INT PRIMARY KEY,
--     emp_name VARCHAR(50) NOT NULL,
--     dept_id INT,
--     salary DECIMAL(10,2),
--     join_date DATE,
--     FOREIGN KEY (dept_id) REFERENCES Department(dept_id)
-- );

-- INSERT INTO Department (dept_id, dept_name) VALUES
-- (101, 'HR'),
-- (102, 'IT'),
-- (103, 'Finance');

-- INSERT INTO Employee (emp_id, emp_name, dept_id, salary, join_date) VALUES
-- (1, 'DK', 101, 6000.00, '2023-09-10'),
-- (2, 'SK', 102, 7000.00, '2024-02-15'),
-- (3, 'AK', 101, 9000.00, '2023-11-20'),
-- (4, 'VK', 103, 8000.00, '2024-01-10'),
-- (5, 'JK', 102, 7500.00, '2023-07-25');


-- SET - 1: Tasks to be Performed:
-- Query to find employees who earn more than the average salary in their respective departments.
-- Query to find Total salary expenditure for each department
-- Query to find Average salary for each department
-- Query to Find the total salary paid per department.
-- Query to find the highest salary in each department
-- Query to find Lowest salary in each department
-- Query to find Count the number of employees in each department


-- SET - 2: Tasks to be Performed: -- Joins with Date and Time Functions:
-- Query to Find all employees who joined within the last 6 months.
-- Query to Find employees who joined in the year 2023
-- Query to Find employees who joined in the last 30 days:
-- Query to Find employees grouped by the month they joined

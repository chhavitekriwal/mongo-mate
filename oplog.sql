CREATE SCHEMA IF NOT EXISTS college_data;
CREATE TABLE IF NOT EXISTS college_data.student (_id text,name text,roll_no integer,is_graduated boolean,date_of_birth timestamptz,created_at timestamptz);
INSERT INTO college_data.student (is_graduated,date_of_birth,created_at,_id,name,roll_no) VALUES (true,'2001-05-18 05:30:00+05:30','2024-05-20 13:20:00+05:30','664b00c4d5e7a232e1483719','XYZ',21);
INSERT INTO college_data.student (_id,name,roll_no,is_graduated,date_of_birth,created_at) VALUES ('664b28b78f0edc72c0213f06','ABC',22,false,'2001-05-18 00:00:00+05:30','2024-05-20 07:50:00+05:30');
UPDATE college_data.student SET roll_no = 25 WHERE _id = '664b28b78f0edc72c0213f06';
UPDATE college_data.student SET roll_no = 29 WHERE _id = '664b00c4d5e7a232e1483719';
INSERT INTO college_data.student (_id,name,roll_no,is_graduated,date_of_birth,created_at) VALUES ('664b31528f0edc72c0213f11','ABC',30,true,'2001-05-17 18:30:00+05:30','2024-05-20 16:47:00+05:30');
UPDATE college_data.student SET roll_no = 35 WHERE _id = '664b31528f0edc72c0213f11';
DELETE FROM college_data.student WHERE _id = '664b28b78f0edc72c0213f06'
CREATE SCHEMA IF NOT EXISTS college_data;
CREATE TABLE IF NOT EXISTS college_data.teacher (_id text,name text,date_of_joining timestamptz,date_of_birth timestamptz);
INSERT INTO college_data.teacher (name,date_of_joining,date_of_birth,_id) VALUES ('Prof ABC','2020-03-18 05:30:00+05:30','1972-04-20 05:30:00+05:30','664de43c2410400c8e9fc2e1');

create table cjol_codes (code varchar(10) unique,description varchar(50));
create table cjol_zone (code varchar(10) unique,name varchar(30));
create table cjol_companys(code varchar(15),name text,industry text,
size varchar(20),nature text,website text,address text,brief text);
create table cjol_jobs(code varchar(15),name text,company_id varchar(15),address varchar(30),
qualifications varchar(16),pay int4range,job_date date,job_class text[],brief text,update_date date);
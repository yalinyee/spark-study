drop table if exists MyUser  ;

create table MyUser(
 id int,
 name varchar(64),
 age int

);

insert into MyUser(id,name,age) value (1,'zhangsan',30);
insert into MyUser(id,name,age) value (2,'lisi',30);
insert into MyUser(id,name,age) value (3,'wangqu',30);
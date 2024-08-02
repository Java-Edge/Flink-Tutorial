create database javaedge_flink;

create table student
(
    id   int(11) NOT NULL AUTO_INCREMENT,
    name varchar(25),
    age  int(10),
    primary key (id)
);
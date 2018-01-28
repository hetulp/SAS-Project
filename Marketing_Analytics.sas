/*panel data preprocessing*/
data panel;
infile "/home/patelhetulk0/sasuser.v94/deod_PANEL_GR_1114_1165.dat" firstobs=2 delimiter='	';
input PANID	WEEK	UNITS	OUTLET$	DOLLARS	IRI_KEY	COLUPC;
run;

data panel2;
set panel;
upc = put(colupc,z13.);
run;

PROC IMPORT DATAFILE='/home/patelhetulk0/sasuser.v94/prod_deod.xls'
	DBMS=XLS
	replace
	OUT=WORK.IMPORT;
	GETNAMES=YES;
RUN;

data b1;
set work.import;
sy1 = sy*1; 
ge1 = ge*1;
vend1 = vend*1;
item1 = item*1;
sy2 = put(sy1,z2.);
vend2 = put(vend1,z5.);
item2 = put(item1,z5.);
key = catt(sy2,ge1,vend2,item2);run;
data brand;
set b1;
keep key l3 l9 product_type l5;
run;
/*merging of panel data with product data*/
proc sql;
create table panel_data as
select a.*,b.l3,b.l9,b.l5,b.Product_type from panel2 a left join brand b on a.upc = b.key order by panid;run;

proc freq data =panel_data;table product_type;run;

data panel_data1;
set panel_data;
if product_type='ANTI PERSPIRANT DEOD';
run;

data b1;
set panel_data1;
oz1 = scan(l9, -1, ' ');
oz2 = COMPRESS(oz1, "OZ");
oz = oz2*1;
drop oz1 oz2 l9 outlet colupc;
price = dollars/(units*oz);
run;

proc sql ;
create table temp as select l3,sum(dollars) as total from b1 group by l3 order by total desc;
run;

data b2;
set b1;
if l3='COLGATE PALMOLIVE' then br=1;
if l3='PROCTER & GAMBLE' then br=2;
if l3='CHURCH & DWIGHT CO INC' then br=3;
if l3='UNILEVER' then br=4;
if br='.' then delete;
run;
proc freq data = b2; table product_type;run;

/*store data preprocessing*/
data d1;
infile "/home/patelhetulk0/sasuser.v94/deod_groc_1114_1165" firstobs=2;
input IRI_KEY WEEK SY GE VEND  ITEM  UNITS DOLLARS  F$    D PR;
run;


data d2;
set d1;
if F = 'NONE' then Fe = 0;else Fe = 1;
if D = 0 then Di = 0;else Di = 1;
run;

data d3;
set d2;
sy1 = sy*1; 
ge1 = ge*1;
vend1 = vend*1;
item1 = item*1;
sy2 = put(sy1,z2.);
vend2 = put(vend1,z5.);
item2 = put(item1,z5.);
key = catt(sy2,ge1,vend2,item2);run;

proc sort data=d3;by descending key;run;
proc sort data=brand;by descending key;run;
/*Merging of store data with panel data*/
proc sql;
create table d4 as
select a.*,b.l3,b.l9,b.product_type from d3 a left join brand b on a.key = b.key;run;

data s1;
set d4;
if product_type='ANTI PERSPIRANT DEOD';
run;

data d5;
set s1;
if l3='COLGATE PALMOLIVE' then br=1;
if l3='PROCTER & GAMBLE' then br=2;
if l3='UNILEVER' then br=4;
if l3='CHURCH & DWIGHT CO INC' then br=3;
if br='.' then delete;
oz1 = scan(l9, -1, ' ');
oz2 = COMPRESS(oz1, "OZ");
oz = oz2*1;
price = dollars/(units*oz);
drop oz1 oz2 l9 l3 F D; 
run;

proc reg data=d5;
model units = price fe di pr;run;

PROC TABULATE DATA=d5 out=d6;
VAR PRICE FE DI PR;
CLASS WEEK IRI_KEY BR;
TABLE WEEK*IRI_KEY*BR,(PRICE FE DI PR)*mean;
RUN;

data d7;
set d6;
if Fe_Mean = 0 then f = 0;else f = 1;
if Di_Mean = 0 then d = 0;else d = 1;
if Pr_Mean = 0 then P = 0;else p = 1;
drop Fe_Mean Di_Mean PR_Mean;
Rename price_Mean = price;
run;

proc sort data=d7;by IRI_Key week;run;

proc transpose data=d7 out=w1 prefix=d;
by IRI_KEY week;
id br;
var d;
run;

proc transpose data=d7 out=w2 prefix=p;
    by IRI_KEY week;
    id br;
    var p;
run;

proc transpose data=d7 out=w3 prefix=f;
    by IRI_KEY week;
    id br;
    var f;
run;

proc transpose data=d7 out=w4 prefix=price;
    by IRI_KEY week;
    id br;
    var price;
run;

data det;
merge w1 w2 w3 w4;
by IRI_KEY week;
run;

data detail;
set det;
keep IRI_KEY week p1-p4 d1-d4 f1-f4 price1-price4;
run;



data detail_2;
set  detail;
if f1='.' then f1=0;
if f2='.' then f2=0;
if f3='.' then f3=0;
if f4='.' then f4=0;
if p1='.' then p1=0;
if p2='.' then p2=0;
if p3='.' then p3=0;
if p4='.' then p4=0;
if d1='.' then d1=0;
if d2='.' then d2=0;
if d3='.' then d3=0;
if d4='.' then d4=0;
if price1='.' then delete;
if price2='.' then delete;
if price3='.' then delete;
if price4='.' then delete;
run;

/*merging panel and store level data*/
proc sql;
create table mdc_data as
select a.*,b.* from b2 a inner join detail_2 b on a.Iri_key = b.IRI_Key and a.week = b.week;run;


PROC IMPORT DATAFILE='/home/patelhetulk0/sasuser.v94/ads demo1 (1).csv'
	DBMS=csv
	replace
	OUT=demo;
	GETNAMES=YES;
RUN;


/*adding demographic variable*/
proc sql;
create table z1 as
select a.*,b.* from mdc_data a left join demo b on a.panid = b.panid order by panid;run;


data deod;
set z1;
keep panid week br d1-d4 p1-p4 f1-f4 price1-price4 incomelevel fsize race resident_type Age_m Edu_m Job_m hour_m Age_f Edu_f Job_f hour_f MaritalStatus Child_age Dogs_num Cats_num;
run;

/*mdc data preprocessing*/
data newdata (keep=panid tid decision mode price display feature loyalty incomelevel fsize Age_m Age_f MaritalStatus Child_age);
set deod;
array pvec{4} p1 - p4; 
array dvec{4} d1 - d4;
array fvec{4} f1 - f4;
array pricevec{4} price1 - price4;
retain tid 0;
tid+1;
do i = 1 to 4;
	mode=i;
	price=pricevec{i};
	display=dvec{i};
	feature=fvec{i};
	discount=pvec{i};
	decision=(br=i);
	output;
end;
run;

data newdata;
set newdata;
br2=0;
br3=0;
br4=0;
if mode = 2 then br2 = 1;
if mode = 3 then br3 = 1;
if mode = 4 then br4 = 1;
inc2=incomelevel*br2;
inc3=incomelevel*br3;
inc4=incomelevel*br4;
nmemb2=fsize*br2;
nmemb3=fsize*br3;
nmemb4=fsize*br4;
Age_m2=Age_m*br2;
Age_m3=Age_m*br3;
Age_m4=Age_m*br4;
Age_f2=Age_f*br2;
Age_f3=Age_f*br3;
Age_f4=Age_f*br4;
MaritalStatus2=MaritalStatus*br2;
MaritalStatus3=MaritalStatus*br3;
MaritalStatus4=MaritalStatus*br4;
Child_age2=Child_age*br2;
Child_age3=Child_age*br3;
Child_age4=Child_age*br4;
run;

/*MDC model*/
proc mdc data=newdata;
model decision = br2 br3 br4 price display feature inc2-inc4 nmemb2-nmemb4 Age_m2-Age_m4 Age_f2-Age_f4/ type=clogit 
	nchoice=4;
	id tid;
	output out=probdata pred=p;
run;

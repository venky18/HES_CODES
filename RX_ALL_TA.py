pyspark --conf spark.executor.memory=14G \
--conf spark.executor.cores=2 \
--conf spark.executor.instances=11 \
--conf spark.yarn.executor.memoryOverhead=1G \
--conf spark.default.parallelism=22 \
--jars  /mnt/zs_edge_s3_direct_output_committer_2.11-1.0.jar

pyspark --conf spark.executor.cores=4 \
--conf spark.executor.memory=12G \
--conf spark.yarn.executor.memoryOverhead=2G \
--conf spark.driver.memory=12G \
--conf spark.driver.cores=4 \
--conf spark.executor.instances=94 \
--conf spark.default.parallelism=752 \
--conf spark.sql.shuffle.partitions=752 \
--conf spark.default.parallelism=752 \
--jars  /mnt/zs_edge_s3_direct_output_committer_2.11-1.0.jar

spark.conf.set('spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version','2')


df = spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/SHS/M_NDC_TA/TA_NDC_Mapping_v3.csv').createOrReplaceTempView("M_NDC_TA")
TA_ARR = ['Auto Immune','CV','Diabates','ID','Neuro','Oncology','COPD','Mental Health']


CURR_TA = "('Auto Immune','ID','Neuro','COPD')"

# CURR_TA = TA_ARR[0]
CURR_TA_S = "AI_N_NE_COPD"



df1 = spark.sql("""SELECT /*+broadcast(b)*/ a.*,b.therapy_area from  hes_stencil.T_OPEN_RX_TXN_DLVR_01 a 
inner join (
		
			select drug_id,therapy_area from hes_stencil.t_drug a 
			inner join 
			(
			SELECT lpad(ndc11,11,00) as ndc11,therapy_area FROM M_NDC_TA where therapy_area in """+CURR_TA+"""
			) b 
			on a.ndc11 = b.ndc11
	) b
	on a.drug_id = b.drug_id
""")


df1.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_01")
df1 = spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_01")


df2 = spark.sql("""SELECT /*+broadcast(b)*/ a.*,b.therapy_area from  hes_stencil.T_OPEN_RX_TXN_DLVR_02 a 
inner join (
		
			select drug_id,therapy_area from hes_stencil.t_drug a 
			inner join 
			(
			SELECT lpad(ndc11,11,00) as ndc11,therapy_area FROM M_NDC_TA where therapy_area in """+CURR_TA+"""
			) b 
			on a.ndc11 = b.ndc11
	) b
	on a.drug_id = b.drug_id
""")

df2.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_02")
df2 = spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_02")

df3 = spark.sql("""SELECT /*+broadcast(b)*/ a.*,b.therapy_area from  hes_stencil.T_OPEN_RX_TXN_DLVR_03 a 
inner join (
		
			select drug_id,therapy_area from hes_stencil.t_drug a 
			inner join 
			(
			SELECT lpad(ndc11,11,00) as ndc11,therapy_area FROM M_NDC_TA where therapy_area in """+CURR_TA+"""
			) b 
			on a.ndc11 = b.ndc11
	) b
	on a.drug_id = b.drug_id
""")
df3.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_03")
df3 = spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/Temp/PRC_03")


df = df1.union(df2).union(df3)

df.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p01Subset")
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/'+CURR_TA_S+'/p01Subset').createOrReplaceTempView("p01Subset")
--



--Phy-Plan level

from pyspark.sql.functions import *
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/'+CURR_TA_S+'/p01Subset').createOrReplaceTempView("p01Subset")
df = spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/SHS/M_DIABATES_PROD_CLASS/DIBATES_PROD_CLASS_MAPPING_v2.csv')

df.select([col(c).alias(c.replace(" ","").replace("#","no").replace("(","").replace(")", "").replace("-","").replace(":","_").replace(".","")) for c in df.columns]).createOrReplaceTempView("M_DIA_PROD_CLAS")
spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/SHS/M_PLAN_CHAN_MAPPING').createOrReplaceTempView("M_PLAN_CHAN_MAPPING")




df = spark.sql("""
	SELECT/*+broadcast(b)*/ physician_id,plan_id,date_format( from_unixtime(unix_timestamp(rx_fill_date, 'MM-dd-yyyy')),'yyyy-MM') month,
	therapy_area,
	b.drug_name,bgi,
	count(distinct claim_id) claim_count,count(distinct patient_id) patient_count,
	sum(patient_pay_norm) as patient_pay_norm , sum(plan_pay_norm) as plan_pay_norm from
	(	
		select *,(patient_pay/days_supply)*30 as patient_pay_norm, (plan_pay/days_supply)*30 as plan_pay_norm from P01SUBSET
		where claim_status = 1 and final_claim_ind = 'Y' and nvl(days_supply,0)  <> 0 and therapy_area = 'Auto Immune'
	) a 
	
	left join (select distinct drug_id,drug_name,bgi from hes_stencil.t_drug) b 
	on a.drug_id = b.drug_id

	group by 1,2,3,4,5,6
""")

df.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p02_phy_plan")
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/'+CURR_TA_S+'/p02_phy_plan').createOrReplaceTempView("p02_phy_plan")

# spark.sql("SELECT SUM(CLAIM_COUNT) from p02_phy_plan").show()

df = spark.sql("""
	
	SELECT /*+broadcast(b,c)*/A.*,b.SPECIALTY_DESCRIPTION,c.*
 	FROM 
	(	
	    SELECT /*+broadcast(b) */physician_id,month,
		Overall_Class,Initial_Class,drug_class,
		drug_name,bgi,
		claim_count,patient_count,patient_pay_norm,plan_pay_norm,
		'DIABATES' AS THERAPY_AREA, b.*	    
	    FROM  p02_phy_plan a
	    left join hes_stencil.t_plan b
		on a.plan_id = b.plan_id

	)  A
	
	LEFT JOIN (SELECT DISTINCT PHYSICIAN_ID, NPI,SPECIALTY_DESCRIPTION FROM HES_STENCIL.T_PHYSICIAN) B
	ON A.PHYSICIAN_ID = B.PHYSICIAN_ID

	left join 
    (
        SELECT /*+ broadcast(b,c,d,e) */a.*,b.hospital_name,c.idn as Hospital_Network_Name,physician_group_name as PG_NAME,network_name as PG_Network_Name,hospital_type,physician_group_type
        ,340b_id_number, 340b_flag
        FROM hes_stencil.p06_phy_hosp_phygroup a
        LEFT JOIN(select distinct definitive_id,hospital_name,hospital_type,340b_id_number,case when 340b_id_number is null then 'No' else 'yes' end as 340b_flag from hes_stencil.dhc_hospital) b ON a.hospital_id = b.definitive_id
        LEFT JOIN(select distinct definitive_idn_id,idn from hes_stencil.dhc_hospital) c ON a.Hospital_Network_ID = c.definitive_idn_id
        LEFT JOIN(select distinct definitive_id,physician_group_name,physician_group_type from hes_stencil.dhc_physician_group) d ON a.pg_id = d.definitive_id 
        LEFT JOIN(select distinct definitive_network_id,network_name from hes_stencil.dhc_physician_group) e ON a.PG_Network_ID = e.definitive_network_id
	) c
    on c.npi = b.npi
""").createOrReplaceTempView("p03_phy_plan_TEMP")

spark.sql("""

SELECT npi,month,Overall_Class,Initial_Class,drug_class,drug_name,bgi,THERAPY_AREA,plan_id,plan_name,plan_type,plan_type_description,
plan_subtype,payment_type,national_insurer_name,national_type,regional_name,regional_type,organization_name,organization_type,admin_name,admin_type,
SPECIALTY_DESCRIPTION,hospital_id,pg_id,hospital_network_id,pg_network_id,hospital_name,Hospital_Network_Name,PG_NAME,PG_Network_Name,hospital_type,
physician_group_type,340b_id_number,340b_flag,
SUM(PATIENT_COUNT) as PATIENT_COUNT, SUM(CLAIM_COUNT) as CLAIM_COUNT,
sum(patient_pay_norm) as patient_pay_norm , sum(plan_pay_norm) as plan_pay_norm
from 
p03_phy_plan_TEMP
GROUP BY npi,month,Overall_Class,Initial_Class,drug_class,drug_name,bgi,claim_count,patient_count,THERAPY_AREA,plan_id,plan_name,plan_type,plan_type_description,
plan_subtype,payment_type,national_insurer_name,national_type,regional_name,regional_type,organization_name,organization_type,admin_name,admin_type,
SPECIALTY_DESCRIPTION,hospital_id,pg_id,hospital_network_id,pg_network_id,hospital_name,Hospital_Network_Name,PG_NAME,PG_Network_Name,hospital_type,
physician_group_type,340b_id_number,340b_flag

""").write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p03_phy_plan")



spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p03_phy_plan").createOrReplaceTempView("p03_phy_plan")

spark.sql("SELECT SUM(CLAIM_COUNT) from p03_phy_plan")

p04_claims_child = spark.sql(""" SELECT 
		MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,
	PLAN_SUBTYPE,PAYMENT_TYPE,NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,
	SPECIALTY_DESCRIPTION,HOSPITAL_ID,PG_ID,HOSPITAL_NETWORK_ID,PG_NETWORK_ID,HOSPITAL_NAME,HOSPITAL_NETWORK_NAME,PG_NAME,PG_NETWORK_NAME,HOSPITAL_TYPE,
	PHYSICIAN_GROUP_TYPE,340B_ID_NUMBER,340B_FLAG,
		SUM(PATIENT_COUNT) PATIENT_COUNT, SUM(CLAIM_COUNT) CLAIM_COUNT,
		SUM(PATIENT_PAY_NORM) AS PATIENT_PAY_NORM , SUM(PLAN_PAY_NORM) AS PLAN_PAY_NORM
		 FROM   p03_phy_plan GROUP BY  
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,
	PLAN_SUBTYPE,PAYMENT_TYPE,NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,
	SPECIALTY_DESCRIPTION,HOSPITAL_ID,PG_ID,HOSPITAL_NETWORK_ID,PG_NETWORK_ID,HOSPITAL_NAME,HOSPITAL_NETWORK_NAME,PG_NAME,PG_NETWORK_NAME,HOSPITAL_TYPE,
	PHYSICIAN_GROUP_TYPE,340B_ID_NUMBER,340B_FLAG
""")
p04_claims_child.cache()
p04_claims_child.show()
p04_claims_child.createOrReplaceTempView("p04_claims_child")

spark.sql("SELECT SUM(CLAIM_COUNT) from p04_claims_child")

# ========
# Claims Splitting

df =spark.sql("""
	SELECT HOSPITAL_ID AS CHILD_ID,HOSPITAL_NAME AS CHILD_NAME,'Hospital' AS CHILD_TYPE,hospital_type AS CHILD_SUBTYPE,HOSPITAL_NETWORK_ID PARENT_ID,HOSPITAL_NETWORK_NAME PARENT_NAME,
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG
	,CLAIM_COUNT,PATIENT_PAY_NORM,PLAN_PAY_NORM
	FROM p04_claims_child WHERE PG_ID IS NULL AND HOSPITAL_ID IS NOT NULL
	 
	UNION ALL

	SELECT PG_ID,PG_NAME,'PG' CHILD_TYPE,PHYSICIAN_GROUP_TYPE,PG_NETWORK_ID,PG_NETWORK_NAME,
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,'Null' as 340B_ID_NUMBER,'Null' as 340B_FLAG,
	CLAIM_COUNT,PATIENT_PAY_NORM,PLAN_PAY_NORM
	FROM p04_claims_child WHERE HOSPITAL_ID IS NULL AND pg_id IS NOT NULL

	UNION ALL

	SELECT PG_ID,PG_NAME,'PG' CHILD_TYPE,PHYSICIAN_GROUP_TYPE,PG_NETWORK_ID,PG_NETWORK_NAME,
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,'Null' as 340B_ID_NUMBER,'Null' as 340B_FLAG,
	CLAIM_COUNT/2,PATIENT_PAY_NORM/2,PLAN_PAY_NORM/2
	FROM p04_claims_child WHERE PG_ID IS not NULL and hospital_id  is not null 

	UNION ALL

	SELECT HOSPITAL_ID AS CHILD_ID,HOSPITAL_NAME AS CHILD_NAME,'Hospital' AS CHILD_TYPE,hospital_type AS CHILD_SUBTYPE,HOSPITAL_NETWORK_ID PARENT_ID,HOSPITAL_NETWORK_NAME PARENT_NAME,
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG,
	CLAIM_COUNT/2,PATIENT_PAY_NORM/2,PLAN_PAY_NORM/2
	FROM p04_claims_child WHERE PG_ID IS not NULL and hospital_id  is not null 

	UNION ALL

	SELECT HOSPITAL_ID AS CHILD_ID,HOSPITAL_NAME AS CHILD_NAME,'Null' AS CHILD_TYPE,hospital_type AS CHILD_SUBTYPE,HOSPITAL_NETWORK_ID PARENT_ID,HOSPITAL_NETWORK_NAME PARENT_NAME,
	MONTH,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG,
	CLAIM_COUNT,PATIENT_PAY_NORM,PLAN_PAY_NORM
	FROM p04_claims_child WHERE PG_ID IS NULL and hospital_id IS null 

""").createOrReplaceTempView("p04_claims_splitting")


spark.sql("SELECT SUM(CLAIM_COUNT) from p04_claims_splitting").show()

df = spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/SHS/M_DIABATES_PROD_CLASS/DIBATES_PROD_CLASS_MAPPING_v2.csv')
df.select([col(c).alias(c.replace(" ","").replace("#","no").replace("(","").replace(")", "").replace("-","").replace(":","_").replace(".","")) for c in df.columns]).createOrReplaceTempView("M_DIA_PROD_CLAS")
spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/SHS/M_PLAN_CHAN_MAPPING').createOrReplaceTempView("M_PLAN_CHAN_MAPPING")

df = spark.read.option('delimiter',',').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Input_Files/DHC/FTD_AFFILIATIONS_JUNE/M_ENT_TYPE_MAPPING')
df.select([col(c).alias(c.replace(" ","").replace("#","no").replace("(","").replace(")", "").replace("-","").replace(":","_").replace(".","")) for c in df.columns]).createOrReplaceTempView("M_ENT_TYPE_MAPPING")

spark.sql("""

	SELECT/*+broadcast(b,c)*/ 
	CHILD_ID,a.CHILD_NAME,CHILD_TYPE,CHILD_SUBTYPE,PARENT_ID,PARENT_NAME,MONTH,
	a.DRUG_NAME,
	BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,a.PLAN_TYPE_DESCRIPTION,a.PLAN_SUBTYPE,a.PAYMENT_TYPE,
	NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,
	ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG,CLAIM_COUNT,PATIENT_PAY_NORM,PLAN_PAY_NORM,
	C.PLAN_CHANNEL_TYPE,OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,
	FINAL_CHILD,CHILD_COT_TYPE,ADDRESS,CITY,STATE,ZIP_CODE,LHM_ID,LHM_NAME,CBSA,CBSA_NAME,CBSA_TYPE,
	ENTITY_ID,ENTITY_NAME,AHRQ_FLAG,ENTITY_FIRM_TYPE,FINAL_ENTITY_TYPE,REFINED_ENTITY_TYPE,IDN_INTEGRATION_LEVEL
	FROM P04_CLAIMS_SPLITTING A

	LEFT JOIN M_PLAN_CHAN_MAPPING C 
	on A.PLAN_TYPE_DESCRIPTION = C.PLAN_TYPE_DESCRIPTION AND A.PLAN_SUBTYPE = C.PLAN_SUBTYPE AND A.PAYMENT_TYPE = C.PAYMENT_TYPE

	LEFT JOIN M_ENT_TYPE_MAPPING C 
	on A.CHILD_ID = C.FINAL_CHILD

""").write.mode('overwrite').option("path","s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p04_claims_splitting").saveAsTable("hes_stencil.p04_RX_DIABATES_CHILD_PLAN_DRUG")


spark.sql("""SELECT
	CHILD_ID,CHILD_NAME,CHILD_TYPE,CHILD_SUBTYPE,PARENT_ID,PARENT_NAME,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,
	PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,
	ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG,PLAN_CHANNEL_TYPE,
	OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,
	FINAL_CHILD,CHILD_COT_TYPE,ADDRESS,CITY,STATE,ZIP_CODE,LHM_ID,LHM_NAME,CBSA,CBSA_NAME,CBSA_TYPE,
	ENTITY_ID,ENTITY_NAME,AHRQ_FLAG,ENTITY_FIRM_TYPE,FINAL_ENTITY_TYPE,REFINED_ENTITY_TYPE,IDN_INTEGRATION_LEVEL,
	sum(CLAIM_COUNT) CLAIM_COUNT, sum(PATIENT_PAY_NORM) PATIENT_PAY_NORM,sum(PLAN_PAY_NORM) PLAN_PAY_NORM from hes_stencil.p04_RX_DIABATES_CHILD_PLAN_DRUG

	where MONTH like '%2019%'

	group by
	CHILD_ID,CHILD_NAME,CHILD_TYPE,CHILD_SUBTYPE,PARENT_ID,PARENT_NAME,DRUG_NAME,BGI,THERAPY_AREA,PLAN_ID,PLAN_NAME,PLAN_TYPE,
	PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,NATIONAL_INSURER_NAME,NATIONAL_TYPE,REGIONAL_NAME,REGIONAL_TYPE,
	ORGANIZATION_NAME,ORGANIZATION_TYPE,ADMIN_NAME,ADMIN_TYPE,SPECIALTY_DESCRIPTION,340B_ID_NUMBER,340B_FLAG,PLAN_CHANNEL_TYPE,
	OVERALL_CLASS,INITIAL_CLASS,DRUG_CLASS,
	FINAL_CHILD,CHILD_COT_TYPE,ADDRESS,CITY,STATE,ZIP_CODE,LHM_ID,LHM_NAME,CBSA,CBSA_NAME,CBSA_TYPE,
	ENTITY_ID,ENTITY_NAME,AHRQ_FLAG,ENTITY_FIRM_TYPE,FINAL_ENTITY_TYPE,REFINED_ENTITY_TYPE,IDN_INTEGRATION_LEVEL
""").write.mode('overwrite').option("path","s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/"+CURR_TA_S+"/p04_RX_DIABATES_CHILD_PLAN_DRUG_2019").saveAsTable("hes_stencil.p04_RX_DIABATES_CHILD_PLAN_DRUG_2019")



++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

------------>-------------------->------------------->-------------------->

++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
spark.sql("SELECT * from p04_claims_splitting ")





spark.sql("select sum(claim_count) from p04_claims_splitting").show()
spark.sql("select sum(claim_count) from p03_phy_plan").show()
========

spark.sql("select sum(patient_count),sum(patient_count) from p03_phy_plan")
spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p04_claims_splitting").createOrReplaceTempView("p04_claims_splitting")

df = spark.sql("""select CHILD_SUBTYPE,PLAN_TYPE_DESCRIPTION,PLAN_SUBTYPE,PAYMENT_TYPE,sum(claim_count) CLAIM_COUNT from p04_claims_splitting group by 1,2,3,4""")
df.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p04_factype_plantype_crosstab")

spark.read.parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p04_factype_plantype_crosstab").coalesce(1).write.mode('overwrite').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/FTD_Exports/p04_factype_plantype_crosstab')

=============================================

df = spark.sql("""select plan_id,count(distinct claim_id) claim_count from p01Subset
	where claim_status = 1 and final_claim_ind = 'Y'
group by plan_id
""")
df.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p02Plans")
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p02Plans').createOrReplaceTempView("p02Plans")


df =spark.sql("""
	SELECT plan_type_description,plan_subtype,payment_type,sum(claim_count) claim_count from p02Plans a
	left join (select distinct plan_id,plan_type_description,plan_subtype,payment_type from hes_stencil.t_plan)b
	on a.plan_id = b.plan_id
	group by 1,2,3
""")

df.write.mode('overwrite').parquet("s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p03Plans")
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p03Plans').createOrReplaceTempView("p03Plans")
spark.read.parquet('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/RX/Dibates/p03Plans').coalesce(1).write.mode('overwrite').option("header","true").csv('s3://aws-a0095-use1-00-d-s3b-shrd-shr-input01/Output_Files/FTD_Exports/p03Plans')

df = spark.sql("""SELECT /*+broadcast(b)*/ drug_name,drug_generic_name,count(claim_id) claim_count from 
	(select drug_id,claim_id from p01Subset
	where claim_status = 1 and final_claim_ind = 'Y') a
	inner join
	(select distinct drug_id,drug_name,drug_generic_name from hes_stencil.t_drug) b
	on a.drug_id = b.drug_id
	group by drug_name,drug_generic_name
	order by claim_count desc

""").show(200,False)



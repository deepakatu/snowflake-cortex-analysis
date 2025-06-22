/*      FOR REFERENCE ONLY

*** NEEDS ap-southeast-2 region for CORTEX support ***

 */

USE ROLE accountadmin;

/*--
• database, schema and warehouse creation
--*/

CREATE OR REPLACE DATABASE tb_voc;

CREATE OR REPLACE SCHEMA tb_voc.raw_pos;
CREATE OR REPLACE SCHEMA tb_voc.raw_support;
CREATE OR REPLACE SCHEMA tb_voc.harmonized;
CREATE OR REPLACE SCHEMA tb_voc.analytics;

CREATE OR REPLACE WAREHOUSE tasty_ds_wh
    WAREHOUSE_SIZE = 'large'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 08
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'data science warehouse for tasty bytes';

USE WAREHOUSE tasty_ds_wh;

CREATE OR REPLACE FILE FORMAT tb_voc.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tb_voc.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/tastybytes-voc/'
file_format = tb_voc.public.csv_ff;

/*-- raw zone table build --*/

CREATE OR REPLACE TABLE tb_voc.raw_pos.menu (...);
CREATE OR REPLACE TABLE tb_voc.raw_pos.truck (...);
CREATE OR REPLACE TABLE tb_voc.raw_pos.order_header (...);
CREATE OR REPLACE TABLE tb_voc.raw_support.truck_reviews (...);

/*-- harmonized and analytics views --*/

CREATE OR REPLACE VIEW tb_voc.harmonized.truck_reviews_v AS
SELECT DISTINCT
    r.review_id, r.order_id, oh.truck_id, r.language, source, r.review, t.primary_city,
    oh.customer_id, TO_DATE(oh.order_ts) AS date, m.truck_brand_name
FROM tb_voc.raw_support.truck_reviews r
JOIN tb_voc.raw_pos.order_header oh ON oh.order_id = r.order_id
JOIN tb_voc.raw_pos.truck t ON t.truck_id = oh.truck_id
JOIN tb_voc.raw_pos.menu m ON m.menu_type_id = t.menu_type_id;

CREATE OR REPLACE VIEW tb_voc.analytics.truck_reviews_v AS
SELECT * FROM harmonized.truck_reviews_v;

/*-- raw zone table load --*/

COPY INTO tb_voc.raw_pos.menu FROM @tb_voc.public.s3load/raw_pos/menu/;
COPY INTO tb_voc.raw_pos.truck FROM @tb_voc.public.s3load/raw_pos/truck/;
COPY INTO tb_voc.raw_pos.order_header FROM @tb_voc.public.s3load/raw_pos/order_header/;
COPY INTO tb_voc.raw_support.truck_reviews FROM @tb_voc.public.s3load/raw_support/truck_reviews/;

ALTER WAREHOUSE tasty_ds_wh SET WAREHOUSE_SIZE = 'Medium';

SELECT 'setup is now complete' AS note;
